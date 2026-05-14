import yaml
from pathlib import Path

def load_config(env:str = "dev") -> dict:
    """
    Load configuration from a YAML file based on the specified environment.

    Args:
        env (str): The environment for which to load the configuration (default: "dev").

    Returns:
        dict: A dictionary containing the loaded configuration.
    """

    config_path = Path(f"/app/configs/{env}.yaml")

    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(config_path, "r") as file:
        config = yaml.safe_load(file)

    return config