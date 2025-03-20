def get_batch_dirs(file_list, prefix):
    batch_dirs = []
    for file in file_list:
        file = file.replace(f"{prefix}/", "")
        files = file.split("/")
        batch_dir = files[0]
        batch_dirs.append(batch_dir)

    batch_dirs = set(batch_dirs)
    return batch_dirs
