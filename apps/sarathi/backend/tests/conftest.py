import os

os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("OPENAI_BASE_URL", "http://provider.invalid/v1")
os.environ.setdefault("OPENAI_API_KEY", "test-key")
os.environ.setdefault("SARATHI_MODELS", "test-model")
