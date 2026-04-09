<template>
  <div class="spark-ruleengine" :class="{ compact: isCompact }">
    <!-- Enlarged (full) mode: 3-panel layout -->
    <template v-if="!isCompact">
      <div class="drag-overlay" v-if="isDragging" :style="{ cursor: dragCursor }"
           @mousemove="onDrag" @mouseup="stopDrag"></div>
      <div class="main-layout">
        <div class="left-panel" :style="{ width: leftWidth + 'px' }">
          <InputColumns
            :columns="columns"
            @insert="insertColumn"
          />
          <FlowVariables
            :variables="flowVariables"
            @insert="insertFlowVariable"
          />
        </div>
        <div class="splitter-v" @mousedown="startDragLeft"></div>
        <div class="center-panel" ref="centerPanel">
          <div class="rule-editor-area" :style="{ height: editorHeight + 'px' }">
            <RuleEditor
              ref="ruleEditor"
              :rules="settings.rules"
              :defaultIsMissing="settings.defaultIsMissing"
              :defaultValue="settings.defaultValue"
              :appendOrReplace="settings.appendOrReplace"
              :outputColumnName="settings.outputColumnName"
              :replaceColumn="settings.replaceColumn"
              :columns="columns"
              @update="onSettingsUpdate"
              @evaluate="evaluateRules"
            />
          </div>
          <div class="splitter-h" @mousedown="startDragCenter"></div>
          <div class="preview-area">
            <OutputPreview
              ref="outputPreview"
              :preview="previewData"
              :error="previewError"
              :loading="isEvaluating"
              :inputPreview="inputPreviewData"
              :inputError="inputPreviewError"
              :inputLoading="isLoadingInput"
              @evaluate="evaluateRules"
              @loadInput="loadInputTable"
            />
          </div>
        </div>
        <div class="splitter-v" @mousedown="startDragRight"></div>
        <div class="right-panel" :style="{ width: rightWidth + 'px' }">
          <FunctionCatalog
            :catalog="functionCatalog"
            @insert="insertTemplate"
          />
        </div>
      </div>
    </template>

    <!-- Compact (side-panel) mode -->
    <template v-else>
      <div class="compact-layout">
        <RuleEditor
          ref="ruleEditor"
          :rules="settings.rules"
          :defaultIsMissing="settings.defaultIsMissing"
          :defaultValue="settings.defaultValue"
          :appendOrReplace="settings.appendOrReplace"
          :outputColumnName="settings.outputColumnName"
          :replaceColumn="settings.replaceColumn"
          :columns="columns"
          :compact="true"
          @update="onSettingsUpdate"
          @evaluate="evaluateRules"
        />
        <OutputPreview
          ref="outputPreview"
          :preview="previewData"
          :error="previewError"
          :loading="isEvaluating"
          :inputPreview="inputPreviewData"
          :inputError="inputPreviewError"
          :inputLoading="isLoadingInput"
          :compact="true"
          @evaluate="evaluateRules"
          @loadInput="loadInputTable"
        />
      </div>
    </template>
  </div>
</template>

<script>
import { ref, reactive, onMounted, onUnmounted } from 'vue'
import { initKnimeService, setApplyListener, callRpc, registerModelSetting, markDirty } from './knimeService.js'
import InputColumns from './components/InputColumns.vue'
import FlowVariables from './components/FlowVariables.vue'
import FunctionCatalog from './components/FunctionCatalog.vue'
import RuleEditor from './components/RuleEditor.vue'
import OutputPreview from './components/OutputPreview.vue'

export default {
  name: 'SparkRuleEngineApp',
  components: { InputColumns, FlowVariables, FunctionCatalog, RuleEditor, OutputPreview },

  setup() {
    const ruleEditor = ref(null)
    const outputPreview = ref(null)
    const centerPanel = ref(null)
    const columns = ref([])
    const flowVariables = ref([])
    const functionCatalog = ref([])
    const previewData = ref('')
    const previewError = ref('')
    const isEvaluating = ref(false)
    const inputPreviewData = ref('')
    const inputPreviewError = ref('')
    const isLoadingInput = ref(false)
    const isCompact = ref(window.innerWidth < 600)

    // Splitter state
    const leftWidth = ref(200)
    const rightWidth = ref(220)
    const editorHeight = ref(Math.round(window.innerHeight * 0.5))
    const isDragging = ref(false)
    const dragCursor = ref('')
    let dragging = null

    const settings = reactive({
      rules: '',
      defaultIsMissing: true,
      defaultValue: '',
      appendOrReplace: 'APPEND',
      outputColumnName: 'Rule Result',
      replaceColumn: ''
    })

    const onResize = () => {
      isCompact.value = window.innerWidth < 600
    }

    // --- Splitter drag handlers ---
    function startDragLeft(e) {
      dragging = 'left'
      isDragging.value = true
      dragCursor.value = 'col-resize'
      e.preventDefault()
    }
    function startDragRight(e) {
      dragging = 'right'
      isDragging.value = true
      dragCursor.value = 'col-resize'
      e.preventDefault()
    }
    function startDragCenter(e) {
      dragging = 'center'
      isDragging.value = true
      dragCursor.value = 'row-resize'
      e.preventDefault()
    }
    function onDrag(e) {
      if (!dragging) return
      e.preventDefault()
      if (dragging === 'left') {
        leftWidth.value = Math.max(100, Math.min(e.clientX, window.innerWidth * 0.4))
      } else if (dragging === 'right') {
        rightWidth.value = Math.max(100, Math.min(window.innerWidth - e.clientX, window.innerWidth * 0.4))
      } else if (dragging === 'center') {
        if (centerPanel.value) {
          const rect = centerPanel.value.getBoundingClientRect()
          editorHeight.value = Math.max(80, Math.min(e.clientY - rect.top, rect.height - 80))
        }
      }
    }
    function stopDrag() {
      dragging = null
      isDragging.value = false
      dragCursor.value = ''
    }

    onMounted(async () => {
      window.addEventListener('resize', onResize)

      const data = await initKnimeService()

      // Load settings from backend
      if (data?.settings) {
        const s = data.settings
        if (s.rules !== undefined) settings.rules = s.rules
        if (s.defaultIsMissing !== undefined) settings.defaultIsMissing = s.defaultIsMissing
        if (s.defaultValue !== undefined) settings.defaultValue = s.defaultValue
        if (s.appendOrReplace !== undefined) settings.appendOrReplace = s.appendOrReplace
        if (s.outputColumnName !== undefined) settings.outputColumnName = s.outputColumnName
        if (s.replaceColumn !== undefined) settings.replaceColumn = s.replaceColumn
      }

      // Load initial data
      const init = data?.initialData || {}
      if (init.columnNamesAndTypes) columns.value = init.columnNamesAndTypes
      if (init.flowVariables) flowVariables.value = init.flowVariables
      if (init.functionCatalog) functionCatalog.value = init.functionCatalog

      registerModelSetting(getSettingsSnapshot())

      setApplyListener(() => getSettingsSnapshot())
    })

    onUnmounted(() => {
      window.removeEventListener('resize', onResize)
    })

    function getSettingsSnapshot() {
      return {
        rules: settings.rules,
        defaultIsMissing: settings.defaultIsMissing,
        defaultValue: settings.defaultValue,
        appendOrReplace: settings.appendOrReplace,
        outputColumnName: settings.outputColumnName,
        replaceColumn: settings.replaceColumn
      }
    }

    function onSettingsUpdate(updated) {
      Object.assign(settings, updated)
      markDirty(getSettingsSnapshot())
    }

    // Convert backtick-wrapped column names to $col$ for Rule Engine syntax
    function insertColumn(text) {
      if (text.startsWith('`') && text.endsWith('`')) {
        text = '$' + text.slice(1, -1) + '$'
      }
      if (ruleEditor.value) {
        ruleEditor.value.insertText(text)
      }
    }

    function insertFlowVariable(text) {
      if (ruleEditor.value) {
        ruleEditor.value.insertText(text)
      }
    }

    function insertTemplate(text) {
      if (ruleEditor.value) {
        ruleEditor.value.insertText(text)
      }
    }

    async function evaluateRules() {
      if (isEvaluating.value) return

      if (outputPreview.value) {
        outputPreview.value.switchToOutput()
      }

      const rulesText = settings.rules
      if (!rulesText || !rulesText.trim()) {
        previewError.value = 'No rules to evaluate. Enter at least one rule.'
        previewData.value = ''
        return
      }

      isEvaluating.value = true
      previewData.value = ''
      previewError.value = ''

      try {
        const result = await callRpc('SparkRuleEngineService', 'evaluateRules', [
          settings.rules,
          settings.defaultValue,
          settings.defaultIsMissing,
          settings.appendOrReplace,
          settings.outputColumnName,
          settings.replaceColumn
        ])

        if (result.success) {
          previewData.value = result.preview || 'Evaluation successful (no preview data).'
          previewError.value = ''
        } else {
          previewData.value = ''
          previewError.value = result.error || 'Unknown error'
        }
      } catch (e) {
        previewError.value = e.message || 'Evaluation failed'
      } finally {
        isEvaluating.value = false
      }
    }

    async function loadInputTable() {
      if (inputPreviewData.value || isLoadingInput.value) return

      isLoadingInput.value = true
      inputPreviewData.value = ''
      inputPreviewError.value = ''

      try {
        const result = await callRpc('SparkRuleEngineService', 'previewInputTable', [])
        if (result.success) {
          inputPreviewData.value = result.preview || 'No data available.'
        } else {
          inputPreviewError.value = result.error || 'Failed to load input data.'
        }
      } catch (e) {
        inputPreviewError.value = e.message || 'Failed to load input data.'
      } finally {
        isLoadingInput.value = false
      }
    }

    return {
      ruleEditor, outputPreview, columns, flowVariables, functionCatalog, settings,
      previewData, previewError, isEvaluating, isCompact,
      inputPreviewData, inputPreviewError, isLoadingInput,
      leftWidth, rightWidth, editorHeight, centerPanel, isDragging, dragCursor,
      onSettingsUpdate, insertColumn, insertFlowVariable, insertTemplate,
      evaluateRules, loadInputTable,
      startDragLeft, startDragRight, startDragCenter, onDrag, stopDrag
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
.spark-ruleengine {
  height: 100%;
  display: flex;
  flex-direction: column;
}

/* Drag overlay */
.drag-overlay {
  position: fixed;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  z-index: 9999;
  user-select: none;
}

/* Enlarged (full) mode */
.main-layout {
  display: flex;
  height: 100%;
  overflow: hidden;
}
.left-panel {
  min-width: 100px;
  display: flex;
  flex-direction: column;
  overflow: hidden;
}
.center-panel {
  flex: 1;
  display: flex;
  flex-direction: column;
  overflow: hidden;
  min-width: 0;
}
.right-panel {
  min-width: 100px;
  display: flex;
  flex-direction: column;
  overflow: hidden;
}

/* Vertical splitter */
.splitter-v {
  width: 5px;
  cursor: col-resize;
  background: #e0e0e0;
  flex-shrink: 0;
  transition: background 0.15s;
}
.splitter-v:hover {
  background: #1e88e5;
}

/* Horizontal splitter */
.splitter-h {
  height: 6px;
  cursor: row-resize;
  background: #e0e0e0;
  flex-shrink: 0;
  position: relative;
  z-index: 10;
  transition: background 0.15s;
}
.splitter-h:hover {
  background: #1e88e5;
}

/* Editor / Preview areas */
.rule-editor-area {
  overflow: hidden;
  display: flex;
  flex-direction: column;
  flex-shrink: 0;
}
.rule-editor-area .rule-editor {
  flex: 1;
  min-height: 0;
}
.preview-area {
  flex: 1;
  min-height: 0;
  overflow: hidden;
  display: flex;
  flex-direction: column;
}
.preview-area .output-preview {
  flex: 1;
  min-height: 0;
}

/* Compact mode */
.compact-layout {
  height: 100%;
  display: flex;
  flex-direction: column;
  padding: 8px;
  overflow: hidden;
}
.compact-layout .rule-editor {
  flex: 1;
  min-height: 0;
}
.compact-layout .output-preview {
  flex: 1;
  min-height: 0;
}
</style>
