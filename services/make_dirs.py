
from pathlib import Path, PurePath

def make_dirs(category,path_year):
    PurePath.joinpath(Path.cwd(),"resources\\",category,path_year).mkdir(parents=True, exist_ok=True)


