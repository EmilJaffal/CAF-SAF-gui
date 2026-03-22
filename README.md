This is the interactive Python application for CAF (https://github.com/bobleesj/composition-analyzer-featurizer). Please follow the instructions provided in https://bobleesj.github.io/composition-analyzer-featurizer to install and run the application.

## Unified CAF + SAF Web GUI

This repository now includes a browser-based runner that combines:

- CAF options 1-5 (same option numbering as CLI)
- SAF feature generation as option 6
- Automatic CAF+SAF run-and-merge as option 7
- SAF-CAF feature performance analysis as option 8 (SVM, PLS-DA, XGBoost)

### Run

1. Install dependencies:

	```bash
	pip install -r requirements.txt
	```

2. Start the web app from this folder:

	```bash
	python web_app.py
	```

3. Open:

	```
	http://localhost:5001
	```

### Input Model

- Upload a `.zip` containing CIF files/folders (required for SAF, optional for CAF).
- Upload extra `.xlsx`/`.csv` files for CAF workflows.
- Select option 1-8.
- For CAF options, optionally provide prompt answers line-by-line to mimic existing CLI prompts.
- For option 8, upload at least one CSV with a structure label column
	(`Structure`, `Structure type`, `Structure_caf`, or `Structure_saf`).
- If the CSV comes from option 7 outputs, use the file ending with `_merged` or `_matched`.

### Output

- The app returns a downloadable ZIP containing generated files.
- `run.log` is included with stdout/stderr and a list of changed output files.

### Multi-repo Requirement

For SAF option 6, this workspace should contain sibling folders:

- `composition-analyzer-featurizer-app`
- `structure-analyzer-featurizer-app`

The web app imports SAF processing from `../structure-analyzer-featurizer-app/main.py`.

For performance option 8, this workspace should also contain:

- `SAF-CAF-performance-main`
