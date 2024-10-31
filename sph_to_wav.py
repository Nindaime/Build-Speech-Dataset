from multiprocessing.managers import SyncManager
import os, re
from sphfile import SPHFile
import pandas as pd
import soundfile as sf

class DirectoryManager:
    def __init__(self, file_path, lock):
        self.file_path = file_path
        self.lock = lock

    def write_wav(self):
        sph_dir = os.path.dirname(self.file_path)
        # Assign path to store audio files converted from sph to wav
        wav_path = os.path.join(os.path.dirname(sph_dir), "wav")
            

        # Thread-safe section to manage folder access
        with self.lock:

            # Create path for wav audio files if it does not exists
            if not os.path.exists(wav_path):
                os.mkdir(wav_path)

        # Convert the SPH file to WAV if it doesn't already exist
        wav_file_path = os.path.join(wav_path, f'{os.path.splitext(os.path.basename(self.file_path))[0]}.wav')
            
        with self.lock:
            if not os.path.exists(wav_file_path):
                sph = SPHFile(self.file_path)
                sph.write_wav(wav_file_path, None, None)

    def segment_audio(self, transcript_df):
        #wav_path = os.path.join(wav_dir, wav_file)
        audio_data, sample_rate = sf.read(self.file_path)

        filename = os.path.splitext(os.path.basename(self.file_path))[0]

        #wav_dir = os.path.normpath(os.path.join(self.file_path, "..", ".."))
        #wav_dir = os.path.dirname(self.file_path)
        output_dir = os.path.join(os.path.normpath(os.path.join(self.file_path, "..", "..")),'wav_segmented')

        with self.lock:
            os.makedirs(output_dir, exist_ok=True)

        for index, row in transcript_df[transcript_df['File'].apply(lambda x: str(x).startswith(filename))].reset_index().iterrows():
            start_time = int(row['Start'] * sample_rate)
            end_time = int(row['End'] * sample_rate)

    #         print(f'Start Time: {start_time}\nEnd Time: {end_time}')

            # Extract the audio segment
            audio_segment = audio_data[start_time:end_time]
    #         print(f'Audio Fragment: {audio_segment}')

            # Save the audio segment as a separate WAV file
            segment_file = f"{row['File']}.wav"
            #segment_file = f'{filename}_Segment{index+1}.wav'
            output_file = os.path.join(output_dir, segment_file)

            print(f'Output File: {output_file}')
            with self.lock:
                #if not os.path.exists(output_file):
                sf.write(output_file, audio_segment, sample_rate)

    def extract_transcript(self):
        stm_filename = f'{os.path.splitext(os.path.basename(self.file_path))[0]}.stm'
            
        stm_filepath = self.file_path
        #segment_count = 1
        rows_to_append = []
        with open(stm_filepath, 'r') as f:
            for line in f:
                line = line.strip()
                _, _, _, start, end, _, transcript = line.split(" ", 6)
                transcript =  self.remove_special_tokens(transcript)
                transcript = self.fix_apostrophe_errors(transcript).strip()
                # Capture the start time, end time and trascript of the stm file into a dictionary
                transcript_dict = {'Start': start, 
                                    'End': end,
                                    'File': f'{os.path.splitext(stm_filename)[0]}',
                                    'Transcript': transcript}
                print(f'File: {stm_filename}\nStart: {start}\nEnd:{end}\n{transcript}\n')

                #segment_count += 1
                rows_to_append.append(transcript_dict)

        transcript_df = pd.DataFrame(rows_to_append)

        transcript_df[['Start', 'End']] = transcript_df[['Start', 'End']].astype(float)
        
        sorted_transcript_df = transcript_df.sort_values(by=['Start']).reset_index(drop=True)

        sorted_transcript_df['File'] = [f"{x}_Segment{index + 1}" for index, x in enumerate(sorted_transcript_df['File'])]

        return sorted_transcript_df


    def fix_apostrophe_errors(self, text):
        return re.sub(r"\b(\w+)\s'([a-zA-Z])", r"\1'\2", text)

    # Step 3: Function to remove special tokens for denoising
    def remove_special_tokens(self, text):
        # Remove tokens in parentheses, and angle brackets
        text = re.sub(r'\(.*?\)', '', text)
        text = re.sub(r'<.*?>', '', text)
        return ' '.join(text.split())

    def del_outliers(self):
        with self.lock:
            if os.path.exists(self.file_path):
                os.remove(self.file_path)
                print(f'OS removed {os.path.splitext(os.path.basename(self.file_path))[0]}.wav')

# Define a helper function to call write_wav
def process_directory_manager_wv(dm):
    dm.write_wav()

def process_directory_manager_et(dm):
    return dm.extract_transcript()

def process_directory_manager_sa(dm, transcript_df):
    dm.segment_audio(transcript_df)

def process_directory_manager_do(dm):
    dm.del_outliers()