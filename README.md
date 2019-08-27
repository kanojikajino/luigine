Luigine
-------

Library for developing an engine using luigi. Key features are:

- auto-naming capability defines the output file name from task parameters.
- `load_output` offers an interface to load the output object from a parental task.
- Automated parameter optimization usign `optuna`

## Dependency
- luigi
- numpy
- optuna
- sklearn

## Installation

```
python setup.py install
```

## Example
See `example.py`.

There are four tasks in `example.py`:

1. `DataPreprocessing` task prepares training and validation data sets.
1. `Train` task, given the result of `DataPreprocessing`, trains a ridge regression model using the training data.
1. `PerformanceEvaluation` task, given the results of the above two tasks, evaluates the model on the validation set.
1. `HyperparameterOptimization` task optimizes the parameters included in `PerformanceEvaluation` task so as to minimize the validation loss.

The following command is used to run `PerformanceEvaluation`:
```
python example.py PerformanceEvaluation --working-dir example_working_dir
```
and the results are stored under `example_working_dir/OUTPUT/eval`.
Log is stored in `ENGLOG/engine.log`

The following command is used to run `HyperparameterOptimization` with 100 trials:
```
python example.py HyperparameterOptimization --working-dir example_working_dir --n-trials 100
```
and the results (sqlite db) are stored under `example_working_dir/OUTPUT/optuna`.
Log is stored in `ENGLOG/engine.log`


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

# Collaborators
- Hiroshi Kajino
- Takeshi Teshima
