def format_excel(df, output_path):
    """
    Format and save DataFrame as Excel file with some basic formatting.
    """
    # Create a Pandas Excel writer using openpyxl as the engine
    writer = df.to_excel(output_path, index=False, engine='openpyxl')
    
    print(f"Excel file saved to {output_path}")
    
    return output_path