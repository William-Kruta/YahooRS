from yahoors import scan_for_csps


data = scan_for_csps(
    "VOO.csv", verbose=True, min_dte=0, max_dte=14, apply_quality_filter=True
)


print(data)
