import angr

file = "/home/dtome/Documents/School/Thesis/code/sst_patch/000015_dump.txt"
obj = angr.Project(file, main_opts={"backend": "blob", "arch": "amd64"})
obj.loader.all_objects
