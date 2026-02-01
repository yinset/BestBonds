from batch_bond_analysis import get_bond_metadata_raw
import json

if __name__ == "__main__":
    symbol = "25国开15"
    print(f"Testing {symbol}...")
    res = get_bond_metadata_raw(symbol)
    print(f"Result: {json.dumps(res, indent=4, ensure_ascii=False)}")
