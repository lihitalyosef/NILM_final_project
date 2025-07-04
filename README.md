# NILM Final Project

This repository contains the code, results, and documentation for our final project on Non-Intrusive Load Monitoring (NILM). The project focuses on identifying household electrical appliances from smart meter data using event-based feature extraction and DBSCAN clustering.

## 📁 Repository Contents

- `final_code.py`: The main Python script for loading MDB files, detecting edges, extracting features, clustering, and identifying devices.
- `classified_events_final.xlsx`: Final results generated by the classification script.
- `known_devices.xlsx`: Signature table of known appliances used for identification.
- `project_report.pdf`: The final written report summarizing the full project, including theoretical background, implementation, analysis, and conclusions.

## 📱 User Application

A live demo web application for users is available at:
👉 [https://app--e-sense-6bbd221a.base44.app](https://app--e-sense-6bbd221a.base44.app)

## ⚙️ Requirements

Make sure the following Python libraries are installed:

```bash
pip install pandas numpy scipy scikit-learn pyodbc openpyxl
```

Additionally, ensure Microsoft Access Database Engine is installed to support reading `.mdb` files via `pyodbc`.

## 📘 Documentation

The final report (PDF) documents the following aspects in detail:
- Feature selection methodology
- Software architecture and logic
- Accuracy and F1-score metrics
- Comparison with related NILM systems in the literature
- Suggestions for future improvements

## 🌐 GitHub Link

This repository is hosted at: [https://github.com/lihitalyosef/NILM_final_project](https://github.com/lihitalyosef/NILM_final_project)

---

For any questions, feel free to reach out!