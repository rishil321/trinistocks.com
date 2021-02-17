# trinistocks.com
A collection of Django/Python apps for displaying stats related to T&amp;T

## Requirements

- Python 3

## Installation instructions for local development

1. Install the virtualenvwrapper package using the command `pip install virtualenvwrapper` on Linux or `pip install virtualenvwrapper-win` on Windows.

2. Clone the repo

3. On a terminal, go to the directory where you cloned the repo and execute the command `mkvirtualenv -p python3 trinistatsenv -r requirements.txt` to create a virtual Python3 environment for this app  and install the packages from the requirements.txt file simultaneously.

4. a.Enter your virtual env using the command `workon trinistatsenv`
   
   b.Use the command `python -V` to verify that you are running Python3 inside this virtualenv.
   
   c.Exit from the virtualenv using the command `deactivate`.

5. The following instructions are for using Visual Studio Code, if you're using a different IDE please refer to its documentation

6. Open VS Code and select 'File>Open Folder' and navigate to folder where you cloned the repo.

7. On the left hand side, click the Extensions button (fifth one). Ensure that Python, Django, django-intellisense and MagicPython are installed.

9. Now click the Run button (fourth one on the left) and click the settings gear icon above the Variables keyword. The launch.json file should open. Paste this code to overwrite that entire file:

```
{
  // Use IntelliSense to learn about possible attributes.
  // Hover to view descriptions of existing attributes.
  // For more information, visit: https://go.microsoft.com/fwlink/?linkid=830387
  "version": "0.2.0",
  "configurations": [
    {
      "name": "Python: Current File",
      "type": "python",
      "request": "launch",
      "program": "${file}",
      "console": "integratedTerminal"
    },
    {
      "name": "Python: Django",
      "type": "python",
      "request": "launch",
      "program": "${workspaceFolder}/trinistats/manage.py",
      "args": ["runserver", "--noreload"],
      "django": true
    }
  ]
}
```

9. Close that file. Now you have two dropdowns in the Run command at the top left. One to run your currently open Python script, and one to run our manage.py script for our Django project.

11. Press 'CTRL + ,' to open the settings window. In the search bar at the top, enter venv and enter '~/.virtualenvs' in the Python:Venv Path input bar. Press enter and restart the IDE.

12. Press the CTRL+SHIFT+P keys and select Python: Select Interpreter. Look for the 'trinistatsenv' environment there. If it does not show up, try other solutions [here](https://stackoverflow.com/questions/54106071/how-to-setup-virtual-environment-for-python-in-vs-code)

13. Now go back to the Run button on the left hand side, select the dropdown, ensure that Python:Django is selected and click the green run button.

14. Check the output of the terminal to see if Django isn't happy about anything.

15. Hopefully everything runs and you can navigate to 127.0.0.1:8000
