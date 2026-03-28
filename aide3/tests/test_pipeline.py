"""Unit tests for AIDE 3 Kubeflow pipeline helpers."""

from pathlib import Path

from aide3.kubeflow.pipeline import (
    compile_pipeline,
    data_prep,
    evaluate_model,
    train_model,
)


def test_data_prep_shapes():
    stats = data_prep(sample_size=1000)
    assert stats["input_rows"] == 1000.0
    assert stats["cleaned_rows"] > 0
    assert stats["train_rows"] > stats["valid_rows"]


def test_train_and_eval_gate():
    prep = data_prep(sample_size=1200)
    metrics = train_model(prep, learning_rate=0.08)
    result = evaluate_model(metrics, min_accuracy=0.82)
    assert "accuracy" in result
    assert "passed" in result
    assert isinstance(result["passed"], bool)


def test_compile_pipeline_file(tmp_path: Path):
    out = tmp_path / "pipeline.yaml"
    compiled = compile_pipeline(output_path=out)
    assert compiled.is_file()
    content = compiled.read_text(encoding="utf-8")
    assert "pipeline" in content.lower()
