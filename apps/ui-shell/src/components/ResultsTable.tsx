import React, { useState } from 'react';
import { Download, Filter, ChevronLeft, ChevronRight } from 'lucide-react';

interface ResultsTableProps {
  data: any[];
}

export default function ResultsTable({ data }: ResultsTableProps) {
  const [page, setPage] = useState(0);
  const [pageSize, setPageSize] = useState(25);
  const [sortColumn, setSortColumn] = useState<string>('');
  const [sortDirection, setSortDirection] = useState<'asc' | 'desc'>('asc');
  const [filter, setFilter] = useState('');

  if (data.length === 0) {
    return (
      <div className="empty-state">
        <p>No query results to display</p>
        <p className="hint">Run a Snowflake query to see results here</p>
      </div>
    );
  }

  const columns = Object.keys(data[0]);
  
  // Apply filtering
  const filteredData = data.filter(row => {
    if (!filter) return true;
    return Object.values(row).some(value => 
      String(value).toLowerCase().includes(filter.toLowerCase())
    );
  });

  // Apply sorting
  const sortedData = [...filteredData].sort((a, b) => {
    if (!sortColumn) return 0;
    
    const aVal = a[sortColumn];
    const bVal = b[sortColumn];
    
    if (aVal === bVal) return 0;
    
    const comparison = aVal < bVal ? -1 : 1;
    return sortDirection === 'asc' ? comparison : -comparison;
  });

  // Apply pagination
  const paginatedData = sortedData.slice(
    page * pageSize, 
    (page + 1) * pageSize
  );

  const totalPages = Math.ceil(sortedData.length / pageSize);

  const handleSort = (column: string) => {
    if (sortColumn === column) {
      setSortDirection(sortDirection === 'asc' ? 'desc' : 'asc');
    } else {
      setSortColumn(column);
      setSortDirection('asc');
    }
  };

  const exportCSV = () => {
    const csv = [
      columns.join(','),
      ...data.map(row => columns.map(col => JSON.stringify(row[col])).join(','))
    ].join('\n');
    
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `results_${Date.now()}.csv`;
    a.click();
  };

  return (
    <div className="results-container">
      <div className="results-header">
        <div className="results-info">
          Showing {paginatedData.length} of {sortedData.length} rows
          {filter && ` (filtered from ${data.length})`}
        </div>
        
        <div className="results-controls">
          <div className="filter-input">
            <Filter size={16} />
            <input
              type="text"
              placeholder="Filter results..."
              value={filter}
              onChange={(e) => setFilter(e.target.value)}
            />
          </div>
          
          <button onClick={exportCSV} className="export-button">
            <Download size={16} />
            Export CSV
          </button>
        </div>
      </div>

      <div className="table-wrapper">
        <table className="results-table">
          <thead>
            <tr>
              {columns.map(column => (
                <th 
                  key={column}
                  onClick={() => handleSort(column)}
                  className={sortColumn === column ? `sorted ${sortDirection}` : ''}
                >
                  {column}
                  {sortColumn === column && (
                    <span className="sort-indicator">
                      {sortDirection === 'asc' ? '▲' : '▼'}
                    </span>
                  )}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {paginatedData.map((row, idx) => (
              <tr key={idx}>
                {columns.map(column => (
                  <td key={column}>
                    {typeof row[column] === 'object' 
                      ? JSON.stringify(row[column])
                      : String(row[column])}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="pagination">
        <button 
          onClick={() => setPage(Math.max(0, page - 1))}
          disabled={page === 0}
        >
          <ChevronLeft size={16} />
        </button>
        
        <span>
          Page {page + 1} of {totalPages}
        </span>
        
        <button 
          onClick={() => setPage(Math.min(totalPages - 1, page + 1))}
          disabled={page === totalPages - 1}
        >
          <ChevronRight size={16} />
        </button>
        
        <select 
          value={pageSize} 
          onChange={(e) => {
            setPageSize(Number(e.target.value));
            setPage(0);
          }}
        >
          <option value={10}>10 rows</option>
          <option value={25}>25 rows</option>
          <option value={50}>50 rows</option>
          <option value={100}>100 rows</option>
        </select>
      </div>
    </div>
  );
}