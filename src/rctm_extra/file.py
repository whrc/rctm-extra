import os
import random
from string import ascii_letters, digits


def generate_hidden_folder(base_dir="."):
    folder_name = "." + "".join(random.choices(ascii_letters + digits, k=10))
    folder_path = os.path.join(base_dir, folder_name)

    os.makedirs(folder_path, exist_ok=True)
    return folder_path
