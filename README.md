Luigine
-------

Library for developing an engine using luigi. Key features are:

- auto-naming capability defines the output file name from task parameters.
- `load_output` offers an interface to load the output object from a parental task.
- Hyperparameter tuning task

## Dependency
- luigi
- numpy
- sklearn

## Installation

```
pip install .
```

## Example
See `example.py`.

There are four tasks in `example.py`:

1. `DataPreprocessing` task prepares training and validation data sets.
1. `Train` task, given the result of `DataPreprocessing`, trains a ridge regression model using the training data.
1. `PerformanceEvaluation` task, given the results of the above two tasks, evaluates the model on the validation set.
1. `HyperparameterOptimization` task optimizes the parameters included in `PerformanceEvaluation` task so as to minimize the validation loss.

The following command is used to run `HyperparameterOptimization` and evaluate the test score:
```
python example.py TestPerformanceEvaluation --working-dir example_working_dir
```
and the results (sqlite db) are stored under `example_working_dir/OUTPUT/TestPerformanceEvaluation`.
Log is stored in `ENGLOG/engine.log`


## How To Use `AutoNamingTask`

1. All tasks must inherit `AutoNamingTask`, instead of `luigi.Task`.
1. Task dependencies are described in `requires` in the same way as `luigi`, except that `requires` must return a **list** of task instances even if it depends on a single task.
1. Task process should be described in `run_task` (instead of `run`), whose
   - input is `input_list`, a list of output Python objects of dependent tasks, and
   - output is Python objects of this task's computation results.
1. The above Python objects are then processed by `save_output` to be pickled and gzipped.
   - If the user wants to choose other data formats, please implement `save_output` and `load_output`, where `save_output` is used to save the output objects of `run_task`, and `load_output` is used to load the Python objects for further processing.
   - A class variable `output_ext` specifies the file extension.
   - Please set it as `output_ext = luigi.Parameter('[your file extension]')`


# Collaborators
- Hiroshi Kajino
- Takeshi Teshima
