# Scheduler walkthrough

In the previous examples, we've seen how using redun, we can define workflows with caching, reactivity, and data provenace all while writing code that looks like typical Python code. So what's the trick? The trick is the use of lazy expressions, which we will explore in detail interactively using a Jupyter notebook.

To try out this example, use Jupyter to execute the notebook `scheduler.ipynb`. A Makefile is provided to install Jupyter if it is not already installed.

```sh
# Install Jupyter.
make setup

# Run the notebook.
make notebook
```

The above command will provide a url to open in your web browser to view the Jupyter notebook.
