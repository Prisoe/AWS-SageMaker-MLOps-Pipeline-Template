import argparse
import json
import os
import pandas as pd
import joblib
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import f1_score

# SageMaker standard locations
SM_CHANNEL_TRAIN = os.environ.get("SM_CHANNEL_TRAIN", "/opt/ml/input/data/train")
SM_CHANNEL_VAL = os.environ.get("SM_CHANNEL_VAL", "/opt/ml/input/data/val")
SM_MODEL_DIR = os.environ.get("SM_MODEL_DIR", "/opt/ml/model")
SM_OUTPUT_DATA_DIR = os.environ.get("SM_OUTPUT_DATA_DIR", "/opt/ml/output/data")

def main():
    parser = argparse.ArgumentParser()

    # Hyperparams (optional, but good for portfolio)
    parser.add_argument("--n-estimators", type=int, default=200)
    parser.add_argument("--random-state", type=int, default=42)

    # Allow overriding channels manually (but defaults work in SageMaker)
    parser.add_argument("--train-dir", type=str, default=SM_CHANNEL_TRAIN)
    parser.add_argument("--val-dir", type=str, default=SM_CHANNEL_VAL)

    args = parser.parse_args()

    train_path = os.path.join(args.train_dir, "train.csv")
    val_path = os.path.join(args.val_dir, "val.csv")

    if not os.path.exists(train_path):
        raise FileNotFoundError(f"Missing train.csv at: {train_path}")
    if not os.path.exists(val_path):
        raise FileNotFoundError(f"Missing val.csv at: {val_path}")

    train_df = pd.read_csv(train_path)
    val_df = pd.read_csv(val_path)

    if "label" not in train_df.columns or "label" not in val_df.columns:
        raise ValueError("Expected a 'label' column in train/val CSVs")

    X_train = train_df.drop(columns=["label"])
    y_train = train_df["label"]
    X_val = val_df.drop(columns=["label"])
    y_val = val_df["label"]

    model = RandomForestClassifier(
        n_estimators=args.n_estimators,
        random_state=args.random_state,
        n_jobs=-1
    )
    model.fit(X_train, y_train)

    preds = model.predict(X_val)
    f1 = f1_score(y_val, preds, average="macro")

    os.makedirs(SM_MODEL_DIR, exist_ok=True)
    joblib.dump(model, os.path.join(SM_MODEL_DIR, "model.joblib"))

    os.makedirs(SM_OUTPUT_DATA_DIR, exist_ok=True)
    metrics = {"macro_f1": float(f1)}
    with open(os.path.join(SM_OUTPUT_DATA_DIR, "metrics.json"), "w") as f:
        json.dump(metrics, f)

    print("✅ Training complete")
    print("Saved model to:", os.path.join(SM_MODEL_DIR, "model.joblib"))
    print("Val macro_f1:", f1)

if __name__ == "__main__":
    main()