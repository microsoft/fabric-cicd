Remove-Item -Recurse -Force dist/*

python -m build

python -m twine upload --repository testpypi dist/*

pip install --upgrade --index-url https://test.pypi.org/simple/ fabric-cicd[dev]
