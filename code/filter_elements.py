import pandas as pd


institutions_csv = "institutions.csv"         # CSV with columns: Institution;Acronym
cordex_csv = "projections-cordex-domains-single-levels.csv"                # CSV with columns: domain, end_year, ensemble_member, ...
output_csv = "cordex_filtered.csv"            # Filtered output CSV


institutions = pd.read_csv(institutions_csv, sep=';')
cordex = pd.read_csv(cordex_csv)


acronyms = institutions["Acronym"].dropna().unique()
acronyms = [a.strip().lower() for a in acronyms]  # normalize to lowercase


def has_institution(rcm_model):
    text = str(rcm_model).lower()
    return any(acr in text for acr in acronyms)

filtered = cordex[cordex["rcm_model"].apply(has_institution)]


filtered.to_csv(output_csv, index=False)

print(f"Saved {len(filtered)} rows to '{output_csv}'.")
