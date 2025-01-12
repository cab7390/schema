use indicatif::ProgressBar;
use memmap2::Mmap;
use rayon::prelude::*;
use simd_json::{to_borrowed_value, BorrowedValue};
use std::fs::File;
use std::io::Result;
use std::path::Path;

/// A type for processing JSON files in parallel using simd-json and user-provided logic.
pub struct ParallelJsonProcessor {
    mmap: Mmap,
    file_size: usize,
    chunk_size: usize,

    progress: ProgressBar,
}

impl ParallelJsonProcessor {
    /// Create a new `ParallelJsonProcessor` from a file.
    pub fn new<P: AsRef<Path>>(path: P, chunk_size: usize) -> Result<Self> {
        // let path = PathBuf::from(filename);
        let file = File::open(path)?;
        let file_size = file.metadata()?.len() as usize;

        eprintln!("Mapping file of size: {}", file_size);

        let mmap = unsafe { memmap2::MmapOptions::new().map(&file)? };

        let progress = ProgressBar::new(file_size as u64);
        progress.set_style(
            indicatif::ProgressStyle::default_bar()
                .template("[{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})")
                .unwrap()
                .progress_chars("#>-"),
        );

        Ok(Self {
            mmap,
            file_size,
            chunk_size,
            progress,
        })
    }

    /// Find chunk boundaries based on newlines.
    fn find_chunk_boundaries(&self) -> Vec<(usize, usize)> {
        let mut boundaries = Vec::new();
        let mut current_start = 0;

        while current_start < self.file_size {
            let tentative_end = current_start
                .saturating_add(self.chunk_size)
                .min(self.file_size);

            if tentative_end >= self.file_size {
                boundaries.push((current_start, self.file_size));
                break;
            }

            let mut actual_end = tentative_end;
            while actual_end < self.file_size && self.mmap[actual_end] != b'\n' {
                actual_end += 1;
            }

            if actual_end >= self.file_size {
                boundaries.push((current_start, self.file_size));
                break;
            } else {
                boundaries.push((current_start, actual_end + 1));
                current_start = actual_end + 1;
            }
        }

        boundaries
    }

    /// Process the JSON file in parallel using a user-provided closure.
    /// The closure processes a single JSON object (BorrowedValue) and returns a result.
    pub fn process<F, T, R>(&self, processor: F, reducer: R) -> T
    where
        F: Fn(&BorrowedValue) -> T + Sync + Send,
        R: Fn(T, T) -> T + Sync + Send + Copy,
        T: Send + Sync + Default,
    {
        let chunk_boundaries = self.find_chunk_boundaries();
        chunk_boundaries
            .into_par_iter()
            .map(|(start, end)| {
                self.process_chunk(&self.mmap[start..end], &processor)
                    .into_iter()
                    .fold(Default::default(), reducer)
            })
            .reduce(Default::default, reducer)
    }

    /// Process a single chunk of JSON data.
    fn process_chunk<F, T>(&self, chunk: &[u8], processor: &F) -> Vec<T>
    where
        F: Fn(&BorrowedValue) -> T,
    {
        let mut results = Vec::new();
        for line in chunk.split(|&b| b == b'\n') {
            if line.is_empty() {
                continue;
            }

            // Parse the line into a BorrowedValue using simd-json.
            let mut owned_line = line.to_vec();
            let borrowed_value = to_borrowed_value(&mut owned_line);
            if let Ok(parsed) = borrowed_value {
                results.push(processor(&parsed));
            }
        }
        results
    }

    pub fn process_with_thread_state<F, R, S>(
        &self,
        processor: F,
        reducer: R,
        state_initializer: impl Fn() -> S,
    ) -> S
    where
        F: Fn(&BorrowedValue, &mut S) + Sync + Send,
        R: Fn(S, S) -> S + Sync + Send,
        S: Default + Clone + Send,
    {
        let chunk_boundaries = self.find_chunk_boundaries();

        let result = chunk_boundaries
            .into_par_iter()
            .fold_with(state_initializer(), |mut local_state, (start, end)| {
                self.process_chunk_with_state(&self.mmap[start..end], &processor, &mut local_state);
                local_state
            })
            .reduce(Default::default, reducer);

        self.progress.finish();

        result
    }

    fn process_chunk_with_state<F, S>(&self, chunk: &[u8], processor: &F, state: &mut S)
    where
        F: Fn(&BorrowedValue, &mut S),
    {
        for line in chunk.split(|&b| b == b'\n') {
            if line.is_empty() {
                continue;
            }

            // Update progress bar
            self.progress.inc(line.len() as u64);

            let mut owned_line = line.to_vec();
            let borrowed_value = to_borrowed_value(&mut owned_line);
            if let Ok(parsed) = borrowed_value {
                processor(&parsed, state);
            }
        }
    }
}
