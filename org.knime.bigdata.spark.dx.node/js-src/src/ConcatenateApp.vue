<template>
  <div class="spark-concatenate">
    <!-- Column Mapping Section -->
    <div class="section">
      <div class="section-header">
        <span class="section-title">Column Mapping</span>
        <button class="btn btn-auto" @click="autoMap" title="Auto-map columns with the same name">Auto Map</button>
      </div>
      <div class="mapping-table-wrapper">
        <table class="mapping-table">
          <thead>
            <tr>
              <th class="col-num">#</th>
              <th class="col-left">Left Column</th>
              <th class="col-right">Right Column</th>
              <th class="col-type">Left Type</th>
              <th class="col-type">Right Type</th>
              <th class="col-action"></th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="(row, idx) in mappings" :key="idx" class="mapping-row">
              <td class="col-num">{{ idx + 1 }}</td>
              <td class="col-left" @click="startEditLeft(idx)">
                <div v-if="editingLeft === idx" class="cell-edit">
                  <select v-model="row.left" @change="onMappingChange" @blur="editingLeft = -1" ref="leftSelects" class="cell-select">
                    <option value="">(not mapped)</option>
                    <option v-for="col in leftCols" :key="col.name" :value="col.name">
                      {{ col.name }}
                    </option>
                  </select>
                </div>
                <div v-else class="cell-display" :class="{ empty: !row.left }">
                  {{ row.left || '(not mapped)' }}
                </div>
              </td>
              <td class="col-right" @click="startEditRight(idx)">
                <div v-if="editingRight === idx" class="cell-edit">
                  <select v-model="row.right" @change="onMappingChange" @blur="editingRight = -1" ref="rightSelects" class="cell-select">
                    <option value="">(not mapped)</option>
                    <option v-for="col in rightCols" :key="col.name" :value="col.name">
                      {{ col.name }}
                    </option>
                  </select>
                </div>
                <div v-else class="cell-display" :class="{ empty: !row.right }">
                  {{ row.right || '(not mapped)' }}
                </div>
              </td>
              <td class="col-type">
                <span class="type-badge" v-if="row.left">{{ getLeftType(row.left) }}</span>
              </td>
              <td class="col-type">
                <span class="type-badge" v-if="row.right">{{ getRightType(row.right) }}</span>
              </td>
              <td class="col-action">
                <button class="btn-remove" @click="removeMapping(idx)" title="Remove mapping">✕</button>
              </td>
            </tr>
            <tr v-if="mappings.length === 0" class="empty-row">
              <td colspan="6" class="empty-msg">
                No mappings defined. Click <b>Auto Map</b> to match same-named columns, or <b>Add Mapping</b> to create one manually.
              </td>
            </tr>
          </tbody>
        </table>
      </div>
      <div class="mapping-actions">
        <button class="btn btn-add" @click="addMapping">+ Add Mapping</button>
        <button class="btn btn-clear" @click="clearMappings" v-if="mappings.length > 0">Clear All</button>
      </div>
    </div>

    <!-- Unmatched Column Handling Section -->
    <div class="section">
      <div class="section-header">
        <span class="section-title">Unmatched Column Handling</span>
      </div>
      <div class="unmatched-options">
        <div class="option-row">
          <label class="option-label">Left columns:</label>
          <div class="radio-group">
            <label class="radio-item">
              <input type="radio" v-model="unmatchedLeft" value="FILL_NULL" @change="onSettingsChange" />
              Include (fill with null)
            </label>
            <label class="radio-item">
              <input type="radio" v-model="unmatchedLeft" value="EXCLUDE" @change="onSettingsChange" />
              Exclude
            </label>
          </div>
        </div>
        <div class="option-row">
          <label class="option-label">Right columns:</label>
          <div class="radio-group">
            <label class="radio-item">
              <input type="radio" v-model="unmatchedRight" value="FILL_NULL" @change="onSettingsChange" />
              Include (fill with null)
            </label>
            <label class="radio-item">
              <input type="radio" v-model="unmatchedRight" value="EXCLUDE" @change="onSettingsChange" />
              Exclude
            </label>
          </div>
        </div>
      </div>
    </div>

    <!-- Summary Section -->
    <div class="section summary-section">
      <div class="section-header">
        <span class="section-title">Summary</span>
      </div>
      <div class="summary-content">
        <div class="summary-row">
          <span class="summary-label">Mapped columns:</span>
          <span class="summary-value">{{ mappings.filter(m => m.left && m.right).length }}</span>
        </div>
        <div class="summary-row" v-if="unmatchedLeftList.length > 0">
          <span class="summary-label">Unmatched left ({{ unmatchedLeft === 'FILL_NULL' ? 'included' : 'excluded' }}):</span>
          <span class="summary-value summary-cols">{{ unmatchedLeftList.join(', ') }}</span>
        </div>
        <div class="summary-row" v-if="unmatchedRightList.length > 0">
          <span class="summary-label">Unmatched right ({{ unmatchedRight === 'FILL_NULL' ? 'included' : 'excluded' }}):</span>
          <span class="summary-value summary-cols">{{ unmatchedRightList.join(', ') }}</span>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import { ref, reactive, computed, onMounted, nextTick } from 'vue'
import { initKnimeService, setApplyListener, registerModelSetting, markDirty } from './knimeService.js'

export default {
  name: 'SparkConcatenateApp',

  setup() {
    const leftCols = ref([])
    const rightCols = ref([])
    const mappings = reactive([])
    const unmatchedLeft = ref('FILL_NULL')
    const unmatchedRight = ref('FILL_NULL')
    const editingLeft = ref(-1)
    const editingRight = ref(-1)
    const leftSelects = ref(null)
    const rightSelects = ref(null)

    // Computed: unmatched column lists for summary
    // Only rows with BOTH left and right are actual mappings
    const unmatchedLeftList = computed(() => {
      const mapped = new Set(mappings.filter(m => m.left && m.right).map(m => m.left))
      return leftCols.value.filter(c => !mapped.has(c.name)).map(c => c.name)
    })

    const unmatchedRightList = computed(() => {
      const mapped = new Set()
      for (const m of mappings) {
        if (m.left && m.right) {
          mapped.add(m.right)
        }
      }
      return rightCols.value.filter(c => !mapped.has(c.name)).map(c => c.name)
    })

    function getLeftType(colName) {
      const col = leftCols.value.find(c => c.name === colName)
      return col ? col.type : ''
    }

    function getRightType(colName) {
      const col = rightCols.value.find(c => c.name === colName)
      return col ? col.type : ''
    }

    function getCurrentSettings() {
      const lefts = []
      const rights = []
      for (const m of mappings) {
        // Send ALL rows in table order (empty string for unmapped cells)
        lefts.push(m.left || '')
        rights.push(m.right || '')
      }
      return {
        leftColumns: lefts,
        rightColumns: rights,
        unmatchedLeftAction: unmatchedLeft.value,
        unmatchedRightAction: unmatchedRight.value
      }
    }

    function onMappingChange() {
      editingLeft.value = -1
      editingRight.value = -1
      onSettingsChange()
    }

    function onSettingsChange() {
      markDirty(getCurrentSettings())
    }

    function autoMap() {
      mappings.length = 0
      const rightNames = new Set(rightCols.value.map(c => c.name))
      const usedRight = new Set()
      // Show ALL left columns in original order; pre-map same-named right columns
      for (const lc of leftCols.value) {
        const rightMatch = rightNames.has(lc.name) ? lc.name : ''
        mappings.push({ left: lc.name, right: rightMatch })
        if (rightMatch) usedRight.add(rightMatch)
      }
      // Append unmatched right columns (left empty)
      for (const rc of rightCols.value) {
        if (!usedRight.has(rc.name)) {
          mappings.push({ left: '', right: rc.name })
        }
      }
      if (mappings.length === 0) {
        mappings.push({ left: '', right: '' })
      }
      onSettingsChange()
    }

    function addMapping() {
      mappings.push({ left: '', right: '' })
    }

    function removeMapping(idx) {
      mappings.splice(idx, 1)
      onSettingsChange()
    }

    function clearMappings() {
      mappings.length = 0
      onSettingsChange()
    }

    function startEditLeft(idx) {
      editingLeft.value = idx
      editingRight.value = -1
      nextTick(() => {
        const selects = document.querySelectorAll('.mapping-table .col-left .cell-select')
        if (selects[0]) selects[0].focus()
      })
    }

    function startEditRight(idx) {
      editingRight.value = idx
      editingLeft.value = -1
      nextTick(() => {
        const selects = document.querySelectorAll('.mapping-table .col-right .cell-select')
        if (selects[0]) selects[0].focus()
      })
    }

    onMounted(async () => {
      const data = await initKnimeService()

      // Load initial data (column lists from both ports)
      const init = data?.initialData || {}
      if (init.leftColumns) leftCols.value = init.leftColumns
      if (init.rightColumns) rightCols.value = init.rightColumns

      // Load saved settings
      const isConfigured = data?.settings?.nodeConfigured
      if (data?.settings) {
        const s = data.settings
        if (s.unmatchedLeftAction) unmatchedLeft.value = s.unmatchedLeftAction
        if (s.unmatchedRightAction) unmatchedRight.value = s.unmatchedRightAction

        if (isConfigured && s.leftColumns?.length) {
          // Build a map of saved mappings: leftCol → rightCol
          const savedMap = new Map()
          for (let i = 0; i < s.leftColumns.length; i++) {
            savedMap.set(s.leftColumns[i], (s.rightColumns && s.rightColumns[i]) || '')
          }
          // Show ALL left columns in original order, with saved right mappings applied
          const usedRight = new Set()
          for (const lc of leftCols.value) {
            const r = savedMap.has(lc.name) ? savedMap.get(lc.name) : ''
            mappings.push({ left: lc.name, right: r })
            if (r) usedRight.add(r)
          }
          // Include any saved left columns not in current left spec (edge case)
          for (const [leftName, rightName] of savedMap) {
            if (!leftCols.value.some(c => c.name === leftName)) {
              mappings.push({ left: leftName, right: rightName })
              if (rightName) usedRight.add(rightName)
            }
          }
          // Append unmatched right columns (left empty)
          for (const rc of rightCols.value) {
            if (!usedRight.has(rc.name)) {
              mappings.push({ left: '', right: rc.name })
            }
          }
        }
      }

      // If no mappings loaded and node was never configured, auto-map
      if (mappings.length === 0 && !isConfigured && leftCols.value.length > 0 && rightCols.value.length > 0) {
        autoMap()
      } else if (mappings.length === 0 && leftCols.value.length > 0) {
        // Node was configured but with no mappings - still show all columns
        for (const lc of leftCols.value) {
          mappings.push({ left: lc.name, right: '' })
        }
        const leftNames = new Set(leftCols.value.map(c => c.name))
        for (const rc of rightCols.value) {
          if (!leftNames.has(rc.name)) {
            mappings.push({ left: '', right: rc.name })
          }
        }
      }

      // Register for dirty-state tracking
      registerModelSetting(getCurrentSettings())

      // Apply listener
      setApplyListener(() => getCurrentSettings())
    })

    return {
      leftCols, rightCols, mappings, unmatchedLeft, unmatchedRight,
      editingLeft, editingRight, leftSelects, rightSelects,
      unmatchedLeftList, unmatchedRightList,
      getLeftType, getRightType, autoMap, addMapping, removeMapping, clearMappings,
      onMappingChange, onSettingsChange, startEditLeft, startEditRight
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
.spark-concatenate {
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
.btn:hover {
  background: #f0f0f0;
  border-color: #999;
}
.btn-auto {
  background: #e3f2fd;
  border-color: #90caf9;
  color: #1565c0;
  font-weight: 500;
}
.btn-auto:hover {
  background: #bbdefb;
}
.btn-add {
  color: #2e7d32;
  border-color: #a5d6a7;
}
.btn-add:hover {
  background: #e8f5e9;
}
.btn-clear {
  color: #c62828;
  border-color: #ef9a9a;
  margin-left: 8px;
}
.btn-clear:hover {
  background: #ffebee;
}

/* Mapping table */
.mapping-table-wrapper {
  overflow-x: auto;
  max-height: 420px;
  overflow-y: auto;
}
.mapping-table {
  width: 100%;
  min-width: 700px;
  border-collapse: collapse;
}
.mapping-table th {
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
.mapping-table td {
  padding: 0;
  border-bottom: 1px solid #f0f0f0;
  vertical-align: middle;
}
.mapping-table .col-num {
  width: 36px;
  text-align: center;
  color: #999;
  font-size: 11px;
  padding: 6px 4px;
}
.mapping-table .col-left,
.mapping-table .col-right {
  min-width: 160px;
  cursor: pointer;
  position: relative;
}
.mapping-table .col-type {
  min-width: 90px;
  padding: 6px 10px;
}
.mapping-table .col-action {
  width: 36px;
  text-align: center;
  padding: 6px 4px;
}

/* Cell display / edit */
.cell-display {
  padding: 7px 10px;
  min-height: 32px;
  transition: background 0.1s;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.cell-display:hover {
  background: #e8f0fe;
}
.cell-display.empty {
  color: #aaa;
  font-style: italic;
}
.cell-edit {
  padding: 2px 4px;
}
.cell-select {
  width: 100%;
  padding: 4px 6px;
  border: 2px solid #1e88e5;
  border-radius: 3px;
  font-size: 13px;
  background: #fff;
  outline: none;
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

/* Remove button */
.btn-remove {
  border: none;
  background: none;
  color: #bbb;
  cursor: pointer;
  font-size: 14px;
  padding: 2px 6px;
  border-radius: 3px;
  transition: all 0.15s;
}
.btn-remove:hover {
  color: #c62828;
  background: #ffebee;
}

/* Empty row */
.empty-row .empty-msg {
  padding: 20px 16px;
  text-align: center;
  color: #888;
  font-style: italic;
}

/* Mapping actions bar */
.mapping-actions {
  padding: 8px 12px;
  background: #fafafa;
  border-top: 1px solid #f0f0f0;
}

/* Row hover */
.mapping-row:hover td {
  background: #f8f9fa;
}

/* Unmatched options */
.unmatched-options {
  padding: 12px;
}
.option-row {
  display: flex;
  align-items: center;
  margin-bottom: 8px;
}
.option-row:last-child {
  margin-bottom: 0;
}
.option-label {
  width: 120px;
  font-weight: 500;
  color: #555;
  flex-shrink: 0;
}
.radio-group {
  display: flex;
  gap: 16px;
}
.radio-item {
  display: flex;
  align-items: center;
  gap: 4px;
  cursor: pointer;
  font-size: 13px;
}
.radio-item input[type="radio"] {
  margin: 0;
  cursor: pointer;
}

/* Summary */
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
.summary-cols {
  color: #555;
  font-weight: 400;
  word-break: break-all;
}
</style>
