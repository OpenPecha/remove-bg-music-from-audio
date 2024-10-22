import os
import subprocess
import shutil
from multiprocessing import Pool, cpu_count

def ensure_directory_exists(dir_path):
    """Ensure the output directory exists."""
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

def separate_audio(input_file, output_dir):
    """Use demucs to separate vocals from the input audio file."""
    command = [
        'demucs',
        '-n', 'htdemucs',  # Use the htdemucs model
        '--two-stems=vocals',  # Separate only vocals and remaining background
        input_file,
        '-o', output_dir
    ]
    subprocess.run(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)  # Suppress output

def rename_vocals_file(original_file, output_dir):
    """Rename and move the vocals file, then resample to 16kHz."""
    original_filename = os.path.basename(original_file).rsplit(".", 1)[0] + ".wav"
    vocals_path = os.path.join(output_dir, 'htdemucs', os.path.basename(
        original_file).rsplit(".", 1)[0], 'vocals.wav')
    new_vocals_path = os.path.join(output_dir, original_filename)

    if os.path.exists(vocals_path):
        shutil.move(vocals_path, new_vocals_path)
        resample_to_16k(new_vocals_path)

def resample_to_16k(audio_file):
    """Resample the audio file to 16kHz using ffmpeg."""
    temp_file = audio_file.replace(".wav", "_16k.wav")
    command = [
        'ffmpeg', '-i', audio_file,
        '-ar', '16000',  # Set sample rate to 16kHz
        '-ac', '1',      # Set to mono channel
        '-threads', str(cpu_count()),  # Utilize all available CPU threads
        temp_file,
        '-y'  # Overwrite output file
    ]
    subprocess.run(command, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    if os.path.exists(temp_file):
        os.remove(audio_file)  # Remove original file
        shutil.move(temp_file, audio_file)  # Replace with the resampled version

def process_single_file(input_file_path, output_dir):
    """Process a single audio file: separate vocals and resample."""
    print(f"Processing {input_file_path}...")
    separate_audio(input_file_path, output_dir)
    rename_vocals_file(input_file_path, output_dir)

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def process_directory(input_dir, output_dir, batch_size=500):
    """Process a directory of audio files in batches with multiprocessing."""
    ensure_directory_exists(output_dir)

    def file_generator():
        """Generate all audio files (wav, mp3) in the input directory."""
        for root, _, files in os.walk(input_dir):
            for filename in files:
                if filename.endswith((".wav", ".mp3")):
                    yield os.path.join(root, filename)

    files_to_process = list(file_generator()) 
    total_files = len(files_to_process)  
    total_batches = (total_files + batch_size - 1) // batch_size  

    for batch_num, batch in enumerate(chunks(files_to_process, batch_size), start=1):
        print(f"Processing batch {batch_num}/{total_batches} with {len(batch)} files...")
        with Pool(processes=max(1, cpu_count() - 1)) as pool: 
            pool.starmap(process_single_file, [(file, output_dir) for file in batch])

        print(f"Completed batch {batch_num}/{total_batches}")

    print(f"All audio files processed! Total files saved: {total_files}")

if __name__ == "__main__":
    input_dir = '../../data/audio_with_bg_music' 
    output_dir = '../../data/cleaned_audio' 
    process_directory(input_dir, output_dir, batch_size=500) 
