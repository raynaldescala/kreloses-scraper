"""
Fix Customer Names - Handle 50 Character Limit
Reads an Excel file with first_name and last_name columns and balances them to fit 50 char limit.

Strategy:
1. If both fit within 50 chars, keep as is
2. If last_name > 50 chars and has multiple words, move first word(s) to first_name
3. If still too long, truncate with ellipsis
"""

import pandas as pd
import sys
import os


def balance_names(first_name, last_name, max_length=50, max_words=3):
    """Balance first and last names to fit within character limits.
    
    Args:
        first_name: Original first name
        last_name: Original last name
        max_length: Maximum characters allowed (default 50)
        max_words: Maximum words allowed in last_name before balancing (default 3)
    
    Returns:
        tuple: (balanced_first_name, balanced_last_name)
    """
    first_name = str(first_name).strip() if pd.notna(first_name) else ''
    last_name = str(last_name).strip() if pd.notna(last_name) else ''
    
    # List of common honorifics (with and without periods)
    honorifics = {'mr', 'mr.', 'ms', 'ms.', 'mrs', 'mrs.', 'miss', 'miss.', 
                  'dr', 'dr.', 'prof', 'prof.', 'sir', 'sir.', 'madam', 'madam.'}
    
    # Check if first_name is just an honorific (strip periods and check lowercase)
    first_name_clean = first_name.lower().strip('.')
    if first_name_clean in honorifics and last_name:
        last_name_words = last_name.split()
        if len(last_name_words) > 1:
            # Move first word of last_name to first_name
            first_word = last_name_words.pop(0)
            first_name = f"{first_name} {first_word}"
            last_name = ' '.join(last_name_words)
    
    last_name_words = last_name.split()
    
    # Balance if last_name has more than max_words OR exceeds max_length
    if len(last_name_words) > max_words or len(last_name) > max_length:
        if len(last_name_words) > 1:
            # Move words from last_name to first_name until it fits criteria
            moved_words = []
            
            # Keep moving words while either condition is true:
            # 1. More than max_words remain
            # 2. Length exceeds max_length
            while len(last_name_words) > 1 and (
                len(last_name_words) > max_words or 
                len(' '.join(last_name_words)) > max_length
            ):
                # Move first word of last_name to first_name
                moved_words.append(last_name_words.pop(0))
            
            # Combine moved words with first_name
            if moved_words:
                new_first_parts = [first_name] + moved_words if first_name else moved_words
                first_name = ' '.join(new_first_parts)
                last_name = ' '.join(last_name_words)
    
    return first_name, last_name


def process_excel_file(input_file, output_file=None):
    """Process Excel file and balance names.
    
    Args:
        input_file: Path to input Excel file
        output_file: Path to output Excel file (optional, defaults to input_file-fixed.xlsx)
    """
    # Read Excel file
    print(f"Reading {input_file}...")
    df = pd.read_excel(input_file)
    
    # Check if required columns exist
    if 'first_name' not in df.columns or 'last_name' not in df.columns:
        print("Error: Excel file must have 'first_name' and 'last_name' columns")
        return
    
    print(f"Processing {len(df)} rows...")
    
    # Track statistics
    stats = {
        'total': len(df),
        'unchanged': 0,
        'balanced': 0
    }
    
    # Process each row
    results = []
    for idx, row in df.iterrows():
        original_first = str(row['first_name']).strip() if pd.notna(row['first_name']) else ''
        original_last = str(row['last_name']).strip() if pd.notna(row['last_name']) else ''
        
        new_first, new_last = balance_names(original_first, original_last)
        
        # Track what happened
        if original_first == new_first and original_last == new_last:
            stats['unchanged'] += 1
            status = 'OK'
        else:
            stats['balanced'] += 1
            status = 'BALANCED'
        
        results.append({
            'first_name': new_first,
            'last_name': new_last,
            'original_first_name': original_first,
            'original_last_name': original_last,
            'status': status,
            'first_name_length': len(new_first),
            'last_name_length': len(new_last)
        })
        
        # Show examples of changes
        if status != 'OK' and stats['balanced'] <= 10:
            print(f"\n  [{status}] Row {idx + 1}:")
            print(f"    Original: '{original_first}' | '{original_last}'")
            print(f"    New:      '{new_first}' ({len(new_first)}) | '{new_last}' ({len(new_last)})")
    
    # Create output DataFrame
    output_df = pd.DataFrame(results)
    
    # Determine output filename
    if not output_file:
        base_name = os.path.splitext(input_file)[0]
        output_file = f"{base_name}-fixed.xlsx"
    
    # Save to Excel
    print(f"\nSaving to {output_file}...")
    output_df.to_excel(output_file, index=False)
    
    # Print statistics
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total rows:       {stats['total']}")
    print(f"Unchanged:        {stats['unchanged']} ({stats['unchanged']/stats['total']*100:.1f}%)")
    print(f"Balanced:         {stats['balanced']} ({stats['balanced']/stats['total']*100:.1f}%)")
    print("=" * 60)
    print(f"\nOutput saved to: {output_file}")
    print("\nColumns in output file:")
    print("  - first_name: Balanced first name (≤50 chars)")
    print("  - last_name: Balanced last name (≤50 chars)")
    print("  - original_first_name: Original first name")
    print("  - original_last_name: Original last name")
    print("  - status: OK or BALANCED")
    print("  - first_name_length: Character count")
    print("  - last_name_length: Character count")


def main():
    """Main entry point"""
    print("=" * 60)
    print("Customer Name Fixer - 50 Character Limit Handler")
    print("=" * 60)
    
    if len(sys.argv) < 2:
        print("\nUsage: python fix_customer_names.py <input_excel_file> [output_excel_file]")
        print("\nExample:")
        print("  python fix_customer_names.py first-name-last-name.xlsx")
        print("  python fix_customer_names.py first-name-last-name.xlsx fixed-names.xlsx")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    
    if not os.path.exists(input_file):
        print(f"Error: File not found: {input_file}")
        sys.exit(1)
    
    print()
    process_excel_file(input_file, output_file)
    print()


if __name__ == "__main__":
    main()
