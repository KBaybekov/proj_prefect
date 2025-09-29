a = ['.vax', '.ss']
good=['ss']
print(next(f[1:] for f in a if f[1:] in good))