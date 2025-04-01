SKA SDP Global Sky Model
========================

This repository contains the code for constructing the global sky model in SKA SDP,
operated as a CLI app implemented in Python.

The `[Documentation] <https://developer.skao.int/projects/ska-sdp-global-sky-model/en/latest/>`__ includes usage
examples, API, and installation directions.

The CI/CD occurs on  `[Gitlab]<https://gitlab.com/ska-telescope/sdp/ska-sdp-global-sky-model.git>`__.

Standard CI machinery
---------------------

This repository is set up to use the
`[Makefiles]<https://gitlab.com/ska-telescope/sdi/ska-cicd-makefile>`_ and `[CI
jobs]<https://gitlab.com/ska-telescope/templates-repository>`_ maintained by the
System Team. For any questions, please look at the documentation in those
repositories or ask for support on Slack in the #team-system-support channel.

To keep the Makefiles up to date in this repository, follow the instructions
at `this link <https://gitlab.com/ska-telescope/sdi/ska-cicd-makefile#keeping-up-to-date>`_.

## Contributing to this repository

`[Black]<https://github.com/psf/black>`_, `[isort]<https://pycqa.github.io/isort/>`_,
and various linting tools are used to keep the Python code in good shape.
Please check that your code follows the formatting rules before committing it
to the repository. You can apply Black and isort to the code with:

.. code-block:: bash
  make python-format

and you can run the linting checks locally using:

.. code-block:: bash
  make python-lint

The linting job in the CI pipeline does the same checks, and it will fail if
the code does not pass all of them.

Creating a new release
----------------------

When you are ready to make a new release (maintainers only):

  - Check out the master branch
  - Update the version number in `.release` with
    - `make bump-patch-release`,
    - `make bump-minor-release`, or
    - `make bump-major-release`
  - Manually replace `main` with the new version number in `CHANGELOG.rst`
  - Create the git tag with `make git-create-tag`
    When it asks for the JIRA ticket, use the ORCA ticket that you are working on
  - Push the changes with `make git-push-tag`
