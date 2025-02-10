import os
import math
import stat

def create_client_script(client_num, start_idx, end_idx, batch_size=1000):
    script_content = f"""#!/bin/bash
echo "Starting Client {client_num} (Records {start_idx} to {end_idx})"
cd /Users/yourpals/Developer/joki_mei
source bin/activate
python3 /Users/yourpals/Developer/joki_mei/scripts/main.py --start {start_idx} --end {end_idx} --client {client_num}
"""
    
    # Create the shell script
    script_path = f'run_client_{client_num}.sh'
    with open(script_path, 'w') as f:
        f.write(script_content)
    
    # Make the script executable
    st = os.stat(script_path)
    os.chmod(script_path, st.st_mode | stat.S_IEXEC)

def generate_clients(total_records, batch_size=1000):
    """
    Generate shell scripts for multiple clients based on total records
    """
    # Create clients directory if it doesn't exist
    os.makedirs('clients', exist_ok=True)
    
    # Calculate number of needed clients
    num_clients = math.ceil(total_records / batch_size)
    
    print(f"Generating {num_clients} client scripts for {total_records} records...")
    
    # Generate a shell script for each client
    for i in range(num_clients):
        start_idx = i * batch_size
        end_idx = min((i + 1) * batch_size, total_records)
        client_num = i + 1
        
        create_client_script(client_num, start_idx, end_idx, batch_size)
        
    # Create a master shell script to run all clients
    master_script = 'run_all_clients.sh'
    with open(master_script, 'w') as f:
        f.write('#!/bin/bash\n')
        f.write('echo "Starting all clients"\n\n')
        
        for i in range(1, num_clients + 1):
            f.write(f'open -a Terminal.app run_client_{i}.sh\n')
        
        f.write('\necho "All clients have been started"\n')
    
    # Make the master script executable
    st = os.stat(master_script)
    os.chmod(master_script, st.st_mode | stat.S_IEXEC)
    
    # Create a results merger script
    with open('merge_results.py', 'w') as f:
        f.write('''import pandas as pd
import glob

def merge_results():
    print("Starting to merge results...")
    
    # Read all batch final files
    batch_files = glob.glob('batch_results/batch_*_final.xlsx')
    
    if not batch_files:
        print("No batch files found to merge!")
        return
        
    print(f"Found {len(batch_files)} batch files to merge")
    
    # Read all dataframes
    dfs = []
    for f in batch_files:
        try:
            df = pd.read_excel(f)
            dfs.append(df)
            print(f"Loaded {f}")
        except Exception as e:
            print(f"Error loading {f}: {str(e)}")
    
    if not dfs:
        print("No data loaded!")
        return
    
    # Concatenate all dataframes
    final_df = pd.concat(dfs)
    
    # Sort by the original index
    final_df = final_df.sort_index()
    
    # Save merged results
    output_file = 'final_merged_results.xlsx'
    final_df.to_excel(output_file, index=False)
    print(f"\\nMerged results saved to {output_file}")

if __name__ == "__main__":
    merge_results()
''')

    # Create a shell script to run the merger
    merger_script = 'merge_results.sh'
    with open(merger_script, 'w') as f:
        f.write('#!/bin/bash\n')
        f.write('python3 merge_results.py\n')
    
    # Make the merger script executable
    st = os.stat(merger_script)
    os.chmod(merger_script, st.st_mode | stat.S_IEXEC)

    print("\nGenerated files:")
    print("1. Individual client shell scripts (run_client_X.sh)")
    print("2. Master shell script (run_all_clients.sh)")
    print("3. Results merger script (merge_results.py)")
    print("4. Results merger shell script (merge_results.sh)")
    
    print("\nInstructions:")
    print("1. To run a single client: Open Terminal and run:")
    print("   ./run_client_X.sh")
    print("2. To run all clients:")
    print("   ./run_all_clients.sh")
    print("3. After all clients finish, merge results:")
    print("   ./merge_results.sh")

if __name__ == "__main__":
    # Example usage
    import pandas as pd
    
    # Read the Excel file to get total records
    try:
        df = pd.read_excel("DATA LAT LONG IDM.xlsx")
        total_records = len(df)
        batch_size = 1000  # Adjust this number based on your needs
        
        generate_clients(total_records, batch_size)
        
    except Exception as e:
        print(f"Error reading Excel file: {str(e)}")