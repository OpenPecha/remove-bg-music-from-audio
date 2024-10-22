import os
import subprocess
import shutil
from multiprocessing import Pool, cpu_count

def ensure_directory_exists(dir_path):
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

def separate_audio(input_file, output_dir):
    command = [
        'demucs',
        '-n', 'htdemucs',
        '--two-stems=vocals',
        input_file,
        '-o', output_dir
    ]
    subprocess.run(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def rename_vocals_file(original_file, output_dir):
    original_filename = os.path.basename(original_file).rsplit(".", 1)[0] + ".wav"
    vocals_path = os.path.join(output_dir, 'htdemucs', os.path.basename(
        original_file).rsplit(".", 1)[0], 'vocals.wav')
    new_vocals_path = os.path.join(output_dir, original_filename)

    if os.path.exists(vocals_path):
        shutil.move(vocals_path, new_vocals_path)
        resample_to_16k(new_vocals_path)

def resample_to_16k(audio_file):
    temp_file = audio_file.replace(".wav", "_16k.wav")
    command = [
        'ffmpeg', '-i', audio_file,
        '-ar', '16000',  # Resample to 16kHz
        '-ac', '1',      # Convert to mono channel
        temp_file,
        '-y'             # Overwrite output file if it exists
    ]
    subprocess.run(command, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    if os.path.exists(temp_file):
        os.remove(audio_file)
        shutil.move(temp_file, audio_file)

def process_single_file(input_file_path, output_dir):
    print(f"Processing {input_file_path}...")
    separate_audio(input_file_path, output_dir)
    rename_vocals_file(input_file_path, output_dir)

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def process_directory(input_dir, output_dir, batch_size=1000):
    ensure_directory_exists(output_dir)

    def file_generator():
        for root, _, files in os.walk(input_dir):
            for filename in files:
                if filename.endswith((".wav", ".mp3")):
                    yield os.path.join(root, filename)

    files_to_process = list(file_generator())

    total_batches = (len(files_to_process) + batch_size - 1) // batch_size

    for batch_num, batch in enumerate(chunks(files_to_process, batch_size), start=1):
        print(f"Processing batch {batch_num}/{total_batches} with {len(batch)} files...")
        with Pool(cpu_count()) as pool:
            pool.starmap(process_single_file, [(file, output_dir) for file in batch])

       
        print(f"Completed batch {batch_num}/{total_batches}")

if __name__ == "__main__":
    input_dir = '../../data/audio_files'
    output_dir = '../../data/cleaned_audio'
    process_directory(input_dir, output_dir, batch_size=1000)  