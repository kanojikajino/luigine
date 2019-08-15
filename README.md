Luigine
-------

Library for developing an engine using luigi. Key features are:

- 
- auto-naming capability defines the output file name from task parameters.
- `load_output` offers an interface to load the output object from a parental task.

## Dependency
- luigi

## Difference from luigi
Task in luigi has three methods to be implemented, `requires`, `output`, and `run`.
In luigine, `output` is already defined in `AutoNamingTask`, and the user does not have to implement it (note that the file extension can be defined by setting a class variable `output_ext = luigi.Parameter('[your file extension]')` in a task class).

Luigine requires us to implement `load_output`, that returns the output object resulting from executing the task.
By this feature, a code block loading the dependent task's output,
```
with gzip.open(self.input().path, 'rb') as f:
    obj = pickle.load(f)
```
could be simplified into the following,
```
obj = self.requires().load_output()
```
where the dependent task has method `load_output` such as
```
def load_output(self):
    with gzip.open(self.input().path, 'rb') as f:
        obj = pickle.load(f)
    return obj
```
