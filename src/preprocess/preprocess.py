import argparse
import os
import pandas as pd
from sklearn.model_selection import train_test_split


def find_csv_path(input_path: str) -> str:
    """SageMaker mounts S3 input either as a directory containing the file or (rarely) as a file."""
    if os.path.isfile(input_path) and input_path.lower().endswith(".csv"):
        return input_path

    if os.path.isdir(input_path):
        files = [f for f in os.listdir(input_path) if f.lower().endswith(".csv")]
        if not files:
            raise FileNotFoundError(f"No CSV found in directory: {input_path}")
        return os.path.join(input_path, files[0])

    raise FileNotFoundError(f"Input path not found: {input_path}")


def safe_stratify(y: pd.Series, min_per_class: int = 2):
    """Return y if stratification is safe; otherwise return None (no stratify)."""
    vc = y.value_counts(dropna=False)
    return y if vc.min() >= min_per_class else None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-data", type=str, required=True)
    parser.add_argument("--output-train", type=str, required=True)
    parser.add_argument("--output-val", type=str, required=True)
    parser.add_argument("--output-test", type=str, required=True)
    args = parser.parse_args()

    csv_path = find_csv_path(args.input_data)
    df = pd.read_csv(csv_path)

    if "label" not in df.columns:
        raise ValueError(f"Expected a 'label' column. Found: {list(df.columns)}")

    # 1) First split: train vs temp (try stratify if safe)
    strat1 = safe_stratify(df["label"], min_per_class=3)
    train_df, temp_df = train_test_split(
        df,
        test_size=0.3,
        random_state=42,
        stratify=strat1,
    )

    # 2) Second split: val vs test (stratify must be safe on temp_df labels)
    strat2 = safe_stratify(temp_df["label"], min_per_class=2)
    val_df, test_df = train_test_split(
        temp_df,
        test_size=0.5,
        random_state=42,
        stratify=strat2,
    )

    os.makedirs(args.output_train, exist_ok=True)
    os.makedirs(args.output_val, exist_ok=True)
    os.makedirs(args.output_test, exist_ok=True)

    train_df.to_csv(os.path.join(args.output_train, "train.csv"), index=False)
    val_df.to_csv(os.path.join(args.output_val, "val.csv"), index=False)
    test_df.to_csv(os.path.join(args.output_test, "test.csv"), index=False)

    print("✅ Split complete")
    print("Train:", train_df.shape, "Val:", val_df.shape, "Test:", test_df.shape)
    print("Label counts (full):", df["label"].value_counts(dropna=False).to_dict())


if __name__ == "__main__":
    main()