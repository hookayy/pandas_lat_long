import pandas as pd
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
    print(f"\nMerged results saved to {output_file}")

if __name__ == "__main__":
    merge_results()
