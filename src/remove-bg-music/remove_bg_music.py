import os
import subprocess
import shutil
import asyncio
from multiprocessing import Pool, cpu_count


def ensure_directory_exists(dir_path):
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)


async def separate_audio(input_file, output_dir, use_gpu=False):
    command = [
        'demucs',
        '-n', 'mdx_extra',
        '--two-stems=vocals',
        input_file,
        '-o', output_dir
    ]
    
    if use_gpu:
        command.append("--device=cuda")
    
    process = await asyncio.create_subprocess_exec(*command)
    await process.wait()


def rename_vocals_file(original_file, output_dir):
    original_filename = os.path.basename(original_file).rsplit(".", 1)[0] + ".wav"
    vocals_path = os.path.join(output_dir, 'mdx_extra', os.path.basename(
        original_file).rsplit(".", 1)[0], 'vocals.wav')
    new_vocals_path = os.path.join(output_dir, original_filename)

    if os.path.exists(vocals_path):
        shutil.move(vocals_path, new_vocals_path)
        resample_to_16k(new_vocals_path)


def resample_to_16k(audio_file):
    temp_file = audio_file.replace(".wav", "_16k.wav")
    command = [
        'ffmpeg', '-i', audio_file, '-ar', '16000', temp_file, '-loglevel', 'error'
    ]
    subprocess.run(command)
    if os.path.exists(temp_file):
        shutil.move(temp_file, audio_file)


def process_single_file(input_file_path, output_dir, use_gpu=False):
    print(f"Processing {input_file_path}...")
    asyncio.run(separate_audio(input_file_path, output_dir, use_gpu))
    rename_vocals_file(input_file_path, output_dir)


def process_directory(input_dir, output_dir, use_gpu=False):
    ensure_directory_exists(output_dir)

    files_to_process = [
        os.path.join(input_dir, filename)
        for filename in os.listdir(input_dir)
        if filename.endswith((".wav", ".mp3"))
    ]

    with Pool(cpu_count() + 2) as pool:
        pool.starmap(process_single_file, [(file, output_dir, use_gpu) for file in files_to_process])


if __name__ == "__main__":
    input_dir = '../../data/audio_files'
    output_dir = '../../data/cleaned_audio'
    

    process_directory(input_dir, output_dir, use_gpu=True)
