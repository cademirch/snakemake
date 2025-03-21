.. _api_reference_snakemake:

The Snakemake API
=================

The Snakemake API consists of three layers.
The first layer is the central entrypoint, given by the :class:`snakemake.api.SnakemakeApi` class.
From this, a workflow can be loaded via the :meth:`snakemake.api.SnakemakeApi.workflow` method, returning the :class:`snakemake.api.WorkflowApi` class.
From this, the DAG can be processes via the :meth:`snakemake.api.WorkflowApi.dag` method, returning the :class:`snakemake.api.DAGApi` class.

All methods and classes are parameterized via Python `dataclasses <https://docs.python.org/3/library/dataclasses.html>`_, defined in :mod:`snakemake.settings`.

It can be used as follows:

.. code-block:: python

    from pathlib import Path

    from snakemake.api import (
        OutputSettings,
        ResourceSettings,
        SnakemakeApi,
        StorageSettings,
    )

    with SnakemakeApi(
        OutputSettings(
            verbose=False,
            show_failed_logs=True,
        ),
    ) as snakemake_api:
        workflow_api = snakemake_api.workflow(
            storage_settings=StorageSettings(),
            resource_settings=ResourceSettings(),
            snakefile=Path("path/to/Snakefile"),
        )
        dag_api = workflow_api.dag()
        # Go on by calling methods of the dag api.


.. autosummary::
   :toctree: _autosummary
   :template: module_template.rst
   :recursive:
   :noindex:

   snakemake.api
   snakemake.settings
   snakemake.settings.enums
   snakemake.settings.types
