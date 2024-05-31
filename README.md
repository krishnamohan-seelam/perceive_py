## perceive_py  
This package is created to learn python concepts 
### Manage repository using poerty
- Install poetry as system-wide application to make it independent of python virtual enviroments 
- To create repo using poetry 
    - poetry new perceive_py  
- To use virtual envs from project  
    - poetry config virtualenvs.in-project true  
- Group Dependencies and Add Extras
    - poetry add --group dev black flake8 isort mypy pylint  
    - poetry add --group test pytest faker  

- Start Development
    - In the project directory, perceieve_py
        - poetry install
        - poetry run python perceive_py/main.py
    - To run unittests
        - poetry run pytest tests
    - To run particular unittest
        - poetry run pytest tests/unit/test_fibanocci_iter.py  

### Iterators and Iterables
- Iterator is an object that allows to iterate over collection of data.In Pythons iterators are implemented using iterator design pattern which allows to traverse a container and access its elements
 
