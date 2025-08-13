# CORDIS JSON to Supabase ETL Pipeline

This project downloads CORDIS (Community Research and Development Information Service) project data from JSON sources and uploads it to a Supabase database. **JSON sources are preferred over CSV for better data accuracy.**

## 🆕 **New JSON-Based Approach**

### **Data Sources (Priority Order):**
1. **🥇 Horizon Europe (PRIMARY)** - `https://cordis.europa.eu/data/cordis-HORIZONprojects-json.zip`
2. **🥈 Horizon 2020** - `https://cordis.europa.eu/data/cordis-h2020projects-json.zip`  
3. **🥉 FP7** - `https://cordis.europa.eu/data/cordis-fp7projects-json.zip`

### **Key Features:**
- ✅ **Smart Data Merging**: Combines multiple JSON files within each dataset
- ✅ **Minimal Nulls**: Intelligent field population to reduce null values
- ✅ **Priority Processing**: Horizon Europe data is primary, others supplement
- ✅ **Robust Error Handling**: Continues processing even if some batches fail
- ✅ **Text Cleaning**: Handles special characters and large text fields

## 🚀 Quick Start

### **Prerequisites Complete ✅**
- Python environment configured
- Dependencies installed  
- Supabase connection tested
- Database table created

### **Run the JSON Pipeline**

```bash
cd /Users/mohan/Development/Projects/Mohan_Research/cordis-to-supabase
./run_json.sh
```

### **Test First (Recommended)**

```bash
# Test JSON processing
/Users/mohan/Development/Projects/Mohan_Research/cordis-to-supabase/.venv/bin/python test_json.py

# Verify schema
/Users/mohan/Development/Projects/Mohan_Research/cordis-to-supabase/.venv/bin/python verify_schema.py
```

## 📊 **Data Processing Strategy**

### **1. Priority-Based Processing**
- **Horizon Europe**: All records included (primary dataset)
- **Horizon 2020**: Only new records not in Horizon Europe
- **FP7**: Only new records not in previous datasets

### **2. Intelligent Field Merging**
- Organizations → `org_names`, `coordinator_names`, `org_countries`
- Topics → `topics_codes`, `topics_desc`
- EuroSciVoc → `euroSciVoc_labels`, `euroSciVoc_codes`
- Web Links → `project_urls`

### **3. Data Quality Improvements**
- Text field cleaning and truncation
- Date standardization (YYYY-MM-DD)
- Numeric field validation
- Null value minimization

## 📁 **Files Overview**

### **JSON Pipeline (New)**
- `cordis_json_to_supabase.py` - Main JSON ETL script ⭐
- `run_json.sh` - JSON pipeline runner ⭐
- `test_json.py` - JSON processing test

### **Legacy CSV Pipeline**
- `cordis_to_supabase.py` - Original CSV ETL script
- `run.sh` - CSV pipeline runner

### **Utilities**
- `verify_schema.py` - Schema verification
- `test_connection.py` - Connection test
- `fixed_schema.sql` - Database schema

## 🔧 **Configuration**

Your setup is already configured:
- **Supabase URL**: `https://bfbhbaipgbazdhghrjho.supabase.co`
- **Service Key**: ✅ Configured
- **Table**: `cordis_projects`

## 📈 **Expected Results**

The JSON pipeline will:
1. **Download** ~500MB of JSON data from EU servers
2. **Process** 70,000+ project records across all programmes
3. **Merge** data intelligently to minimize null values
4. **Upload** in batches of 50 records to avoid timeouts
5. **Provide** detailed progress and error reporting

## ⚡ **Performance Improvements**

- **Smaller batches** (50 vs 100) for better reliability
- **Enhanced error recovery** - continues on failed batches
- **Text field optimization** - handles large descriptions
- **Memory efficient** - processes datasets sequentially

## 🎯 **Why JSON is Better**

| Aspect | CSV | JSON ✅ |
|--------|-----|---------|
| **Data Structure** | Flat, limited | Nested, rich |
| **Data Types** | All strings | Native types |
| **Relationships** | Separate files | Embedded |
| **Encoding** | Semicolon issues | UTF-8 native |
| **Parsing** | Complex joins | Direct mapping |

## 🚀 **Ready to Run!**

```bash
# Run the complete JSON pipeline
./run_json.sh
```

**Expected runtime**: 15-30 minutes depending on network speed.

---

## 🆘 **Troubleshooting**

If you encounter issues:

1. **Test connectivity**: `python test_connection.py`
2. **Verify schema**: `python verify_schema.py`  
3. **Test JSON processing**: `python test_json.py`
4. **Check logs**: Look for specific error messages in terminal output

**Questions?** The pipeline provides detailed progress and error reporting! 🎉
