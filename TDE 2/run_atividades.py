import subprocess
import os

def run_atividade(atividade_number):
    script_name = f'Atividade{atividade_number}.py'
    input_file = 'dataset.csv'
    output_file = f'Atividade{atividade_number}_output_new'

    print(f'Running {script_name}...')

    # Check if the Atividade script exists
    if not os.path.exists(script_name):
        print(f'Error: {script_name} not found.')
        return

    # Run the MapReduce job
    subprocess.run([
        'python', script_name,
        input_file,
        '--output', output_file
    ])

    print(f'Output for Atividade {atividade_number} saved to {output_file}\n')

if __name__ == '__main__':
        run_atividade(6)
