import csv
import os
import requests
from multiprocessing import Pool, cpu_count
from pydub import AudioSegment


def create_output_dir(output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)


def read_csv_files(input_dir):
    csv_rows = []
    file_counts = {}
    try:
        for file_name in os.listdir(input_dir):
            if file_name.endswith('.csv'):
                file_path = os.path.join(input_dir, file_name)
                with open(file_path, newline='', encoding='utf-8') as csvfile:
                    reader = csv.DictReader(csvfile)
                    rows = list(reader)
                    csv_rows.extend(rows)
                    file_counts[file_name] = len(rows)
    except Exception as e:
        print(f"Error reading CSV files: {e}")
    return csv_rows, file_counts


def download_audio(row, output_dir):
    file_name = row['file_name'] + '.wav'
    url = row['url']
    file_path = os.path.join(output_dir, file_name)
    if os.path.exists(file_path):
        print(f"Skipping {file_name}, already exists.")
        return 0
    try:
        response = requests.get(url)
        response.raise_for_status()
        with open(file_path, 'wb') as file:
            file.write(response.content)
        print(f"Downloaded: {file_name}")
        convert_to_16kHz(file_path)
        return 1
    except Exception as e:
        print(f"Failed to download {file_name}: {e}")
        return 0


def convert_to_16kHz(file_path):
    try:
        audio = AudioSegment.from_wav(file_path)
        audio = audio.set_frame_rate(16000)
        audio.export(file_path, format='wav')
    except Exception as e:
        print(f"Failed to convert {file_path} to 16 kHz: {e}")


def download_audios_from_csv(csv_rows, output_dir):
    if not csv_rows: 
        print("No audio files to download. Exiting...")
        return
    total_downloaded = 0
    pool_size = min(cpu_count(), len(csv_rows))
    with Pool(pool_size) as pool:
        results = pool.starmap(download_audio, [(row, output_dir) for row in csv_rows])
        total_downloaded = sum(results)
    return total_downloaded


def main(input_dir, output_dir):
    create_output_dir(output_dir)
    csv_rows, file_counts = read_csv_files(input_dir)
    for file_name, count in file_counts.items():
        print(f"{file_name}: {count} audio files found.")
    total_downloaded = download_audios_from_csv(csv_rows, output_dir)
    print(f"Download completed! Total audio files downloaded: {total_downloaded}")


if __name__ == "__main__":
    input_dir = '../../data/audio_files'
    output_dir = '../../data/audio_files'
    main(input_dir, output_dir)
