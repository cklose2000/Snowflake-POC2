// Artifact Preview - Generate preview for large datasets
export class ArtifactPreview {
  constructor(options = {}) {
    this.maxRows = options.maxRows || 10;
    this.maxCellLength = options.maxCellLength || 100;
    this.includeStats = options.includeStats !== false;
  }

  generatePreview(data) {
    if (!Array.isArray(data) || data.length === 0) {
      return {
        preview: [],
        stats: null,
        message: 'No data to preview'
      };
    }

    const preview = {
      rows: this.getPreviewRows(data),
      columns: this.getColumnInfo(data),
      stats: this.includeStats ? this.calculateStats(data) : null,
      totalRows: data.length,
      previewRows: Math.min(this.maxRows, data.length)
    };

    return preview;
  }

  getPreviewRows(data) {
    const previewData = data.slice(0, this.maxRows);
    
    return previewData.map(row => {
      const previewRow = {};
      for (const [key, value] of Object.entries(row)) {
        previewRow[key] = this.truncateValue(value);
      }
      return previewRow;
    });
  }

  truncateValue(value) {
    if (value === null || value === undefined) {
      return null;
    }

    const strValue = typeof value === 'object' 
      ? JSON.stringify(value) 
      : String(value);

    if (strValue.length > this.maxCellLength) {
      return strValue.substring(0, this.maxCellLength - 3) + '...';
    }

    return value;
  }

  getColumnInfo(data) {
    if (data.length === 0) return [];

    const columns = {};
    const sampleSize = Math.min(100, data.length);
    const sample = data.slice(0, sampleSize);

    // Analyze column types and nullability
    for (const row of sample) {
      for (const [key, value] of Object.entries(row)) {
        if (!columns[key]) {
          columns[key] = {
            name: key,
            types: new Set(),
            nullCount: 0,
            uniqueValues: new Set(),
            minLength: Infinity,
            maxLength: 0
          };
        }

        if (value === null || value === undefined) {
          columns[key].nullCount++;
        } else {
          columns[key].types.add(typeof value);
          columns[key].uniqueValues.add(JSON.stringify(value));
          
          const strLength = String(value).length;
          columns[key].minLength = Math.min(columns[key].minLength, strLength);
          columns[key].maxLength = Math.max(columns[key].maxLength, strLength);
        }
      }
    }

    // Format column information
    return Object.values(columns).map(col => ({
      name: col.name,
      type: this.inferColumnType(col.types),
      nullable: col.nullCount > 0,
      nullPercentage: (col.nullCount / sampleSize) * 100,
      cardinality: col.uniqueValues.size,
      minLength: col.minLength === Infinity ? 0 : col.minLength,
      maxLength: col.maxLength
    }));
  }

  inferColumnType(types) {
    const typeArray = Array.from(types);
    
    if (typeArray.length === 0) return 'NULL';
    if (typeArray.length === 1) return typeArray[0].toUpperCase();
    
    // Mixed types
    if (typeArray.includes('number') && typeArray.includes('string')) {
      return 'VARIANT';
    }
    
    return 'MIXED';
  }

  calculateStats(data) {
    const stats = {
      rowCount: data.length,
      columnCount: data.length > 0 ? Object.keys(data[0]).length : 0,
      estimatedSize: JSON.stringify(data).length,
      numericalColumns: [],
      categoricalColumns: [],
      dateColumns: []
    };

    if (data.length === 0) return stats;

    const columns = this.getColumnInfo(data);
    
    for (const col of columns) {
      if (col.type === 'NUMBER') {
        const values = data
          .map(row => row[col.name])
          .filter(v => v !== null && v !== undefined)
          .map(Number);
        
        if (values.length > 0) {
          stats.numericalColumns.push({
            name: col.name,
            min: Math.min(...values),
            max: Math.max(...values),
            avg: values.reduce((a, b) => a + b, 0) / values.length,
            median: this.calculateMedian(values)
          });
        }
      } else if (col.cardinality < 50) {
        // Likely categorical
        const valueCounts = {};
        for (const row of data) {
          const value = row[col.name];
          if (value !== null && value !== undefined) {
            valueCounts[value] = (valueCounts[value] || 0) + 1;
          }
        }
        
        stats.categoricalColumns.push({
          name: col.name,
          uniqueValues: col.cardinality,
          topValues: Object.entries(valueCounts)
            .sort((a, b) => b[1] - a[1])
            .slice(0, 5)
            .map(([value, count]) => ({ value, count }))
        });
      }
      
      // Check for date columns
      if (this.isDateColumn(col.name, data)) {
        const dates = data
          .map(row => row[col.name])
          .filter(v => v !== null && v !== undefined)
          .map(d => new Date(d));
        
        if (dates.length > 0) {
          stats.dateColumns.push({
            name: col.name,
            min: new Date(Math.min(...dates)),
            max: new Date(Math.max(...dates))
          });
        }
      }
    }

    return stats;
  }

  calculateMedian(values) {
    const sorted = values.slice().sort((a, b) => a - b);
    const middle = Math.floor(sorted.length / 2);
    
    if (sorted.length % 2 === 0) {
      return (sorted[middle - 1] + sorted[middle]) / 2;
    }
    
    return sorted[middle];
  }

  isDateColumn(columnName, data) {
    // Check if column name suggests it's a date
    const datePatterns = /date|time|timestamp|created|updated|modified/i;
    if (datePatterns.test(columnName)) {
      return true;
    }
    
    // Sample first non-null value
    for (const row of data) {
      const value = row[columnName];
      if (value !== null && value !== undefined) {
        // Try to parse as date
        const date = new Date(value);
        return !isNaN(date.getTime());
      }
    }
    
    return false;
  }

  formatForDisplay(preview) {
    // Format preview for console or UI display
    const display = {
      summary: `Showing ${preview.previewRows} of ${preview.totalRows} rows`,
      columns: preview.columns.map(col => 
        `${col.name} (${col.type}${col.nullable ? ', nullable' : ''})`
      ),
      data: preview.rows
    };

    if (preview.stats) {
      display.statistics = {
        size: `~${(preview.stats.estimatedSize / 1024).toFixed(2)} KB`,
        numerical: preview.stats.numericalColumns.map(col =>
          `${col.name}: min=${col.min}, max=${col.max}, avg=${col.avg.toFixed(2)}`
        ),
        categorical: preview.stats.categoricalColumns.map(col =>
          `${col.name}: ${col.uniqueValues} unique values`
        )
      };
    }

    return display;
  }
}

export default ArtifactPreview;