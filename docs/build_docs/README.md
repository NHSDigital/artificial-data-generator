
# How do I add user documentation to the production DAE?
There are 2 steps to building the documentation and adding it to DAE:
1. (Desktop Python) Run the python file `notebooks/user/build_docs/make_create_user_docs.py`: the full documentation exists in the `docs` folder in the top-level of the repo; this step takes this documentation and puts the contents into a file that can be run on DAE to make it readable by users.
1. (DAE Prod) Copy the notebook `notebooks/user/collab/create_user_docs.py` created in step 1 into DAE Prod and run it: this will add the documentation to a user-facing table in DAE Prod.
