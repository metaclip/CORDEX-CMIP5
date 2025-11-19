import pandas as pd

# === INPUT/OUTPUT CONFIGURATION ===
institutions_csv = "institutions.csv"         # CSV with columns: Institution;Acronym
cordex_csv = "projections-cordex-domains-single-levels.csv"                # CSV with columns: domain, end_year, ensemble_member, ...
output_csv = "cordex_filtered.csv"            # Filtered output CSV

# === 1. LOAD DATA ===
institutions = pd.read_csv(institutions_csv, sep=';')
cordex = pd.read_csv(cordex_csv)

# === 2. EXTRACT LIST OF ACRONYMS ===
acronyms = institutions["Acronym"].dropna().unique()
acronyms = [a.strip().lower() for a in acronyms]  # normalize to lowercase

# === 3. FILTER ROWS WHERE rcm_model CONTAINS AN ACRONYM ===
def has_institution(rcm_model):
    text = str(rcm_model).lower()
    return any(acr in text for acr in acronyms)

filtered = cordex[cordex["rcm_model"].apply(has_institution)]

# === 4. SAVE RESULT ===
filtered.to_csv(output_csv, index=False)

print(f"✅ Filtering completed. Saved {len(filtered)} rows to '{output_csv}'.")
