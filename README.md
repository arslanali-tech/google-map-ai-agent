# 📍 Google Maps Business Details Scraper with Gemini API

An intelligent Python-based tool that automates the extraction of business details from Google Maps using **Playwright** and enhances data parsing with the **Gemini API**.

---

## 🔍 Description

This scraper collects business details such as:

- Business Name  
- Business Type  
- Address  
- Phone Number  
- Email  
- Website  

The data is cleaned, structured, and exported to an Excel `.xlsx` file for easy analysis.

---

## ⚙️ Features

✅ Automated Google Maps search  
✅ Intelligent parsing via Gemini API  
✅ Graceful handling of missing or partial data  
✅ Exports results to Excel  
✅ Optional GUI for control (Start, Stop Scrolling, Stop All)  
✅ Built-in error handling and rate limit management  

---

## 🛠️ Technologies Used

- Python 3.8+  
- [Playwright (async)](https://playwright.dev/python/docs/intro)  
- [Gemini API (Google Generative AI)](https://ai.google.dev)  
- `httpx`, `pandas`, `tkinter`, `dotenv`

---

## 🔑 Requirements

- Google Gemini API Key  
- Install dependencies with:

```bash
pip install -r requirements.txt
playwright install
