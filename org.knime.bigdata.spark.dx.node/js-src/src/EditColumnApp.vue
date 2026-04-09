<template>
  <div class="spark-editcolumn">
    <!-- Column Editing Section -->
    <div class="section">
      <div class="section-header">
        <span class="section-title">Column Editing</span>
        <div class="header-actions">
          <button class="btn btn-reset" @click="resetAll" title="Reset all edits to original values">Reset All</button>
        </div>
      </div>
      <div class="table-wrapper">
        <table class="edit-table">
          <thead>
            <tr>
              <th class="col-handle"></th>
              <th class="col-num">#</th>
              <th class="col-name">Column Name</th>
              <th class="col-newname">New Name</th>
              <th class="col-type">Type</th>
              <th class="col-newtype">New Type</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="(col, idx) in columns" :key="col.name"
                class="edit-row"
                :class="{ 'row-selected': selectedIndex === idx, 'row-modified': isModified(col), 'row-dragging': dragIndex === idx, 'row-dragover': dragOverIndex === idx }"
                draggable="true"
                @click="selectedIndex = idx"
                @dragstart="onDragStart(idx, $event)"
                @dragover.prevent="onDragOver(idx)"
                @dragend="onDragEnd"
                @drop.prevent="onDrop(idx, $event)">
              <td class="col-handle" title="Drag to reorder">
                <span class="drag-icon">&#x2630;</span>
              </td>
              <td class="col-num">{{ idx + 1 }}</td>
              <td class="col-name">
                <span class="name-text">{{ col.name }}</span>
              </td>
              <td class="col-newname">
                <input type="text" class="cell-input"
                       v-model="col.newName"
                       @input="onSettingsChange"
                       :placeholder="col.name"
                       title="Leave empty to keep original name" />
              </td>
              <td class="col-type">
                <span class="type-badge">{{ col.type }}</span>
              </td>
              <td class="col-newtype">
                <select class="cell-select" v-model="col.newType" @change="onSettingsChange">
                  <option value="">(keep)</option>
                  <option value="STRING">STRING</option>
                  <option value="INTEGER">INTEGER</option>
                  <option value="LONG">LONG</option>
                  <option value="DOUBLE">DOUBLE</option>
                  <option value="FLOAT">FLOAT</option>
                  <option value="BOOLEAN">BOOLEAN</option>
                  <option value="DATE">DATE</option>
                  <option value="TIMESTAMP">TIMESTAMP</option>
                </select>
              </td>
            </tr>
            <tr v-if="columns.length === 0" class="empty-row">
              <td colspan="6" class="empty-msg">
                No input columns available. Connect and configure the upstream node.
              </td>
            </tr>
          </tbody>
        </table>
      </div>
      <div class="table-footer">
        <div class="move-buttons">
          <button class="btn btn-move" @click="moveUp" :disabled="selectedIndex <= 0" title="Move selected row up">&#9650; Up</button>
          <button class="btn btn-move" @click="moveDown" :disabled="selectedIndex < 0 || selectedIndex >= columns.length - 1" title="Move selected row down">&#9660; Down</button>
        </div>
        <span class="footer-hint">Drag rows or use Up/Down to reorder. Click a row to select it.</span>
      </div>
    </div>

    <!-- Summary Section -->
    <div class="section summary-section">
      <div class="section-header">
        <span class="section-title">Summary</span>
      </div>
      <div class="summary-content">
        <div class="summary-row">
          <span class="summary-label">Total columns:</span>
          <span class="summary-value">{{ columns.length }}</span>
        </div>
        <div class="summary-row" v-if="renameCount > 0">
          <span class="summary-label">Renames ({{ renameCount }}):</span>
          <span class="summary-value summary-detail">{{ renameSummary }}</span>
        </div>
        <div class="summary-row" v-if="castCount > 0">
          <span class="summary-label">Type casts ({{ castCount }}):</span>
          <span class="summary-value summary-detail">{{ castSummary }}</span>
        </div>
        <div class="summary-row" v-if="reorderChanged">
          <span class="summary-label">Order:</span>
          <span class="summary-value summary-detail">Changed from original</span>
        </div>
        <div class="summary-row" v-if="renameCount === 0 && castCount === 0 && !reorderChanged">
          <span class="summary-value no-changes">No changes configured.</span>
        </div>
        <div class="summary-row float-note" v-if="hasFloat">
          <span class="summary-detail">Note: FLOAT is mapped to Double in KNIME.</span>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import { ref, reactive, computed, onMounted } from 'vue'
import { initKnimeService, setApplyListener, registerModelSetting, markDirty } from './knimeService.js'

export default {
  name: 'SparkEditColumnApp',

  setup() {
    const columns = reactive([])
    const selectedIndex = ref(-1)
    const dragIndex = ref(-1)
    const dragOverIndex = ref(-1)
    const originalOrder = ref([])

    // ── Computed summaries ────────────────────────────────────────────

    const renameCount = computed(() => columns.filter(c => c.newName && c.newName.trim()).length)
    const castCount = computed(() => columns.filter(c => c.newType).length)
    const hasFloat = computed(() => columns.some(c => c.newType === 'FLOAT'))

    const renameSummary = computed(() => {
      return columns
        .filter(c => c.newName && c.newName.trim())
        .map(c => c.name + ' \u2192 ' + c.newName.trim())
        .join(', ')
    })

    const castSummary = computed(() => {
      return columns
        .filter(c => c.newType)
        .map(c => c.name + ': ' + c.type + ' \u2192 ' + c.newType)
        .join(', ')
    })

    const reorderChanged = computed(() => {
      if (columns.length !== originalOrder.value.length) return false
      for (let i = 0; i < columns.length; i++) {
        if (columns[i].name !== originalOrder.value[i]) return true
      }
      return false
    })

    // ── Settings helpers ─────────────────────────────────────────────

    function getCurrentSettings() {
      const src = []
      const names = []
      const types = []
      for (const col of columns) {
        src.push(col.name)
        names.push(col.newName || '')
        types.push(col.newType || '')
      }
      return { sourceColumns: src, newNames: names, newTypes: types }
    }

    function onSettingsChange() {
      markDirty(getCurrentSettings())
    }

    function isModified(col) {
      return (col.newName && col.newName.trim()) || col.newType
    }

    // ── Row actions ──────────────────────────────────────────────────

    function moveUp() {
      const i = selectedIndex.value
      if (i > 0) {
        const temp = columns[i]
        columns.splice(i, 1)
        columns.splice(i - 1, 0, temp)
        selectedIndex.value = i - 1
        onSettingsChange()
      }
    }

    function moveDown() {
      const i = selectedIndex.value
      if (i >= 0 && i < columns.length - 1) {
        const temp = columns[i]
        columns.splice(i, 1)
        columns.splice(i + 1, 0, temp)
        selectedIndex.value = i + 1
        onSettingsChange()
      }
    }

    function resetAll() {
      for (const col of columns) {
        col.newName = ''
        col.newType = ''
      }
      // Restore original order only if current columns match original set
      const currentNames = new Set(columns.map(c => c.name))
      const originalNames = new Set(originalOrder.value)
      if (currentNames.size === originalNames.size && [...currentNames].every(n => originalNames.has(n))) {
        const colMap = {}
        for (const col of columns) {
          colMap[col.name] = col
        }
        columns.length = 0
        for (const name of originalOrder.value) {
          if (colMap[name]) {
            columns.push(colMap[name])
          }
        }
      }
      onSettingsChange()
    }

    // ── Drag & drop ──────────────────────────────────────────────────

    function onDragStart(idx, event) {
      dragIndex.value = idx
      selectedIndex.value = idx
      event.dataTransfer.effectAllowed = 'move'
      event.dataTransfer.setData('text/plain', idx.toString())
    }

    function onDragOver(idx) {
      dragOverIndex.value = idx
    }

    function onDragEnd() {
      dragOverIndex.value = -1
      // dragIndex is reset in onDrop; if drop did not fire, reset here
      setTimeout(() => { dragIndex.value = -1 }, 0)
    }

    function onDrop(targetIdx, event) {
      // Use dataTransfer as reliable source index (Firefox fires dragend before drop)
      let sourceIdx = dragIndex.value
      if (sourceIdx < 0 && event && event.dataTransfer) {
        sourceIdx = parseInt(event.dataTransfer.getData('text/plain'), 10)
      }
      if (isNaN(sourceIdx) || sourceIdx < 0 || sourceIdx === targetIdx) return
      const item = columns[sourceIdx]
      columns.splice(sourceIdx, 1)
      columns.splice(targetIdx, 0, item)
      selectedIndex.value = targetIdx
      dragIndex.value = -1
      dragOverIndex.value = -1
      onSettingsChange()
    }

    // ── Initialization ───────────────────────────────────────────────

    onMounted(async () => {
      const data = await initKnimeService()

      // Load input columns from port
      const init = data?.initialData || {}
      const inputCols = init.inputColumns || init.columnNamesAndTypes || []

      // Load saved settings
      const settings = data?.settings || {}
      const savedSrc = settings.sourceColumns || []
      const savedNames = settings.newNames || []
      const savedTypes = settings.newTypes || []

      // Build column lookup from input
      const inputMap = {}
      for (const ic of inputCols) {
        inputMap[ic.name] = ic.type
      }

      // Merge: saved columns first (preserving saved order), then remaining input columns
      const addedNames = new Set()

      for (let i = 0; i < savedSrc.length; i++) {
        const name = savedSrc[i]
        if (inputMap[name] !== undefined && !addedNames.has(name)) {
          columns.push({
            name: name,
            type: inputMap[name],
            newName: savedNames[i] || '',
            newType: savedTypes[i] || ''
          })
          addedNames.add(name)
        }
      }

      // Remaining input columns not in saved settings
      for (const ic of inputCols) {
        if (!addedNames.has(ic.name)) {
          columns.push({
            name: ic.name,
            type: ic.type,
            newName: '',
            newType: ''
          })
          addedNames.add(ic.name)
        }
      }

      // Store original input order for reorder detection
      originalOrder.value = inputCols.map(c => c.name)

      // Register settings
      registerModelSetting(getCurrentSettings())
      setApplyListener(() => getCurrentSettings())
    })

    return {
      columns, selectedIndex, dragIndex, dragOverIndex,
      renameCount, castCount, hasFloat, renameSummary, castSummary, reorderChanged,
      isModified, getCurrentSettings, onSettingsChange,
      moveUp, moveDown, resetAll,
      onDragStart, onDragOver, onDragEnd, onDrop
    }
  }
}
</script>

<style>
* {
  margin: 0;
  padding: 0;
  box-sizing: border-box;
}
html, body, #app {
  height: 100%;
  width: 100%;
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
  font-size: 13px;
  color: #333;
  background: #fff;
}
.spark-editcolumn {
  height: 100%;
  overflow-y: auto;
  padding: 12px;
}

/* Section layout */
.section {
  margin-bottom: 16px;
  border: 1px solid #e0e0e0;
  border-radius: 6px;
  overflow: hidden;
}
.section-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 8px 12px;
  background: #f5f7fa;
  border-bottom: 1px solid #e0e0e0;
}
.section-title {
  font-weight: 600;
  font-size: 13px;
  color: #1a1a1a;
}

/* Buttons */
.btn {
  display: inline-flex;
  align-items: center;
  padding: 4px 12px;
  border: 1px solid #ccc;
  border-radius: 4px;
  background: #fff;
  font-size: 12px;
  cursor: pointer;
  transition: all 0.15s;
}
.btn:hover:not(:disabled) {
  background: #f0f0f0;
  border-color: #999;
}
.btn:disabled {
  opacity: 0.4;
  cursor: default;
}
.btn-reset {
  color: #c62828;
  border-color: #ef9a9a;
}
.btn-reset:hover {
  background: #ffebee;
}
.btn-move {
  padding: 3px 10px;
  font-size: 11px;
}

/* Table */
.table-wrapper {
  overflow-x: auto;
  max-height: 420px;
  overflow-y: auto;
}
.edit-table {
  width: 100%;
  min-width: 740px;
  border-collapse: collapse;
}
.edit-table th {
  padding: 6px 10px;
  text-align: left;
  font-size: 11px;
  font-weight: 600;
  color: #666;
  text-transform: uppercase;
  letter-spacing: 0.3px;
  background: #fafafa;
  border-bottom: 1px solid #e0e0e0;
  position: sticky;
  top: 0;
  z-index: 1;
}
.edit-table td {
  padding: 0;
  border-bottom: 1px solid #f0f0f0;
  vertical-align: middle;
}

/* Column widths */
.col-handle { min-width: 28px; width: 28px; text-align: center; cursor: grab; }
.col-num { min-width: 36px; width: 36px; text-align: center; color: #999; font-size: 11px; padding: 6px 2px; }
.col-name { min-width: 140px; padding: 6px 10px; }
.col-newname { min-width: 140px; }
.col-type { min-width: 100px; padding: 6px 10px; }
.col-newtype { min-width: 130px; }

/* Drag handle */
.drag-icon {
  color: #ccc;
  font-size: 14px;
  cursor: grab;
  user-select: none;
}
.drag-icon:hover { color: #888; }

/* Row states */
.edit-row { transition: background 0.1s; cursor: pointer; }
.edit-row:hover td { background: #f8f9fa; }
.edit-row.row-selected td { background: #e3f2fd; }
.edit-row.row-selected:hover td { background: #bbdefb; }
.edit-row.row-modified td { background: #fffde7; }
.edit-row.row-modified:hover td { background: #fff9c4; }
.edit-row.row-dragging { opacity: 0.4; }
.edit-row.row-dragover td { border-top: 2px solid #1e88e5; }

/* Name column */
.name-text {
  font-weight: 500;
  color: #1a1a1a;
}

/* Type badge */
.type-badge {
  display: inline-block;
  padding: 1px 6px;
  border-radius: 3px;
  font-size: 11px;
  color: #546e7a;
  background: #eceff1;
}

/* Cell input */
.cell-input {
  width: 100%;
  padding: 6px 10px;
  border: none;
  border-left: 1px solid #f0f0f0;
  font-size: 13px;
  font-family: inherit;
  background: transparent;
  outline: none;
  transition: background 0.1s;
}
.cell-input:focus {
  background: #e8f0fe;
  border-left-color: #1e88e5;
}
.cell-input::placeholder {
  color: #ccc;
  font-style: italic;
}

/* Cell select */
.cell-select {
  width: 100%;
  padding: 5px 8px;
  border: none;
  border-left: 1px solid #f0f0f0;
  font-size: 13px;
  font-family: inherit;
  background: transparent;
  cursor: pointer;
  outline: none;
}
.cell-select:focus {
  background: #e8f0fe;
}

/* Table footer */
.table-footer {
  display: flex;
  align-items: center;
  padding: 8px 12px;
  background: #fafafa;
  border-top: 1px solid #f0f0f0;
  gap: 12px;
}
.move-buttons {
  display: flex;
  gap: 4px;
}
.footer-hint {
  color: #999;
  font-size: 11px;
}

/* Empty row */
.empty-row .empty-msg {
  padding: 24px 16px;
  text-align: center;
  color: #888;
  font-style: italic;
}

/* Summary section */
.summary-section {
  background: #fafafa;
}
.summary-content {
  padding: 10px 12px;
}
.summary-row {
  display: flex;
  margin-bottom: 4px;
  font-size: 12px;
}
.summary-row:last-child {
  margin-bottom: 0;
}
.summary-label {
  color: #666;
  margin-right: 6px;
  flex-shrink: 0;
}
.summary-value {
  font-weight: 500;
}
.summary-detail {
  color: #555;
  font-weight: 400;
  word-break: break-all;
}
.no-changes {
  color: #999;
  font-style: italic;
  font-weight: 400;
}
.float-note {
  margin-top: 4px;
  color: #b71c1c;
  font-size: 11px;
}
.float-note .summary-detail {
  color: #b71c1c;
}
</style>
