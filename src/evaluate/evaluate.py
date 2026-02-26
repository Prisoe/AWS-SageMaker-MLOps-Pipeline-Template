import argparse
import json
import os
import tarfile
import pandas as pd
import joblib
from sklearn.metrics import classification_report, f1_score


def _extract_model_artifact(model_dir: str) -> str:
    """
    In SageMaker Pipelines, the training step model artifact is provided as model.tar.gz.
    We extract it and return the directory to load model.joblib from.
    """
    tar_path = os.path.join(model_dir, "model.tar.gz")

    # Sometimes the tarball can be nested; but default is /opt/ml/processing/model/model.tar.gz
    if not os.path.exists(tar_path):
        return model_dir

    extract_dir = os.path.join(model_dir, "extracted")
    os.makedirs(extract_dir, exist_ok=True)

    with tarfile.open(tar_path, "r:gz") as tar:
        tar.extractall(path=extract_dir)

    return extract_dir


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", type=str, required=True)
    parser.add_argument("--test", type=str, required=True)
    parser.add_argument("--output-dir", type=str, required=True)
    args = parser.parse_args()

    model_root = _extract_model_artifact(args.model)

    # train.py saves model.joblib at the root of the model directory in SM_MODEL_DIR
    model_path = os.path.join(model_root, "model.joblib")

    # defensive: sometimes artifacts nest under /model/
    if not os.path.exists(model_path):
        alt = os.path.join(model_root, "model", "model.joblib")
        if os.path.exists(alt):
            model_path = alt
        else:
            raise FileNotFoundError(
                f"model.joblib not found. Tried: {model_path} and {alt}. "
                f"Contents of {model_root}: {os.listdir(model_root)}"
            )

    model = joblib.load(model_path)

    test_csv = os.path.join(args.test, "test.csv")
    if not os.path.exists(test_csv):
        raise FileNotFoundError(f"Missing test.csv at: {test_csv}")

    test_df = pd.read_csv(test_csv)

    if "label" not in test_df.columns:
        raise ValueError("Expected a 'label' column in test.csv")

    X_test = test_df.drop(columns=["label"])
    y_test = test_df["label"]

    preds = model.predict(X_test)
    f1 = f1_score(y_test, preds, average="macro")

    report = classification_report(y_test, preds, output_dict=True)
    output = {"macro_f1": float(f1), "report": report}

    os.makedirs(args.output_dir, exist_ok=True)
    out_path = os.path.join(args.output_dir, "evaluation.json")
    with open(out_path, "w") as f:
        json.dump(output, f)

    print("✅ Evaluation complete")
    print("macro_f1:", f1)
    print("wrote:", out_path)


if __name__ == "__main__":
    main()