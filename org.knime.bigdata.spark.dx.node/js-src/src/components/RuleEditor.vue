<template>
  <div class="rule-editor" :class="{ compact: compact }">
    <!-- Rule editor header -->
    <div class="editor-header">
      <span class="editor-title">Rules</span>
      <span class="editor-hint">condition =&gt; outcome (one per line, // for comments)</span>
    </div>

    <!-- Rule text area -->
    <div class="editor-body">
      <textarea
        ref="textarea"
        class="rule-textarea"
        :value="rules"
        @input="onRulesInput"
        @keydown="onKeyDown"
        @drop="onDrop"
        @dragover.prevent
        placeholder="$age$ > 60 => &quot;Senior&quot;&#10;$age$ > 18 => &quot;Adult&quot;&#10;TRUE => &quot;Minor&quot;"
        spellcheck="false"
        wrap="off"
      ></textarea>
    </div>

    <!-- Output configuration -->
    <div class="output-config">
      <div class="config-row">
        <label class="config-label">
          <input
            type="checkbox"
            :checked="defaultIsMissing"
            @change="onDefaultIsMissingChange"
          />
          Default value is MISSING
        </label>
      </div>

      <div class="config-row" v-if="!defaultIsMissing">
        <span class="config-label-text">Default value:</span>
        <input
          type="text"
          class="config-input"
          :value="defaultValue"
          @input="onDefaultValueInput"
          placeholder="Enter default value"
        />
      </div>

      <div class="config-row">
        <span class="config-label-text">Output mode:</span>
        <div class="mode-switch">
          <button
            class="mode-btn"
            :class="{ active: appendOrReplace === 'APPEND' }"
            @click="onModeChange('APPEND')"
          >Append</button>
          <button
            class="mode-btn"
            :class="{ active: appendOrReplace === 'REPLACE' }"
            @click="onModeChange('REPLACE')"
          >Replace</button>
        </div>
      </div>

      <div class="config-row" v-if="appendOrReplace === 'APPEND'">
        <span class="config-label-text">Column name:</span>
        <input
          type="text"
          class="config-input"
          :value="outputColumnName"
          @input="onOutputColumnNameInput"
          placeholder="Rule Result"
        />
      </div>

      <div class="config-row" v-if="appendOrReplace === 'REPLACE'">
        <span class="config-label-text">Replace column:</span>
        <select class="config-select" :value="replaceColumn" @change="onReplaceColumnChange">
          <option value="">-- Select column --</option>
          <option v-for="col in columns" :key="col.name" :value="col.name">
            {{ col.name }}
          </option>
        </select>
      </div>
    </div>
  </div>
</template>

<script>
import { ref } from 'vue'

export default {
  name: 'RuleEditor',
  props: {
    rules: { type: String, default: '' },
    defaultIsMissing: { type: Boolean, default: true },
    defaultValue: { type: String, default: '' },
    appendOrReplace: { type: String, default: 'APPEND' },
    outputColumnName: { type: String, default: 'Rule Result' },
    replaceColumn: { type: String, default: '' },
    columns: { type: Array, default: () => [] },
    compact: { type: Boolean, default: false }
  },
  emits: ['update', 'evaluate'],
  setup(props, { emit }) {
    const textarea = ref(null)

    function emitUpdate(overrides) {
      emit('update', {
        rules: props.rules,
        defaultIsMissing: props.defaultIsMissing,
        defaultValue: props.defaultValue,
        appendOrReplace: props.appendOrReplace,
        outputColumnName: props.outputColumnName,
        replaceColumn: props.replaceColumn,
        ...overrides
      })
    }

    function onRulesInput(e) {
      emitUpdate({ rules: e.target.value })
    }

    function onDefaultIsMissingChange(e) {
      emitUpdate({ defaultIsMissing: e.target.checked })
    }

    function onDefaultValueInput(e) {
      emitUpdate({ defaultValue: e.target.value })
    }

    function onModeChange(mode) {
      emitUpdate({ appendOrReplace: mode })
    }

    function onOutputColumnNameInput(e) {
      emitUpdate({ outputColumnName: e.target.value })
    }

    function onReplaceColumnChange(e) {
      emitUpdate({ replaceColumn: e.target.value })
    }

    function onKeyDown(e) {
      // Tab → insert 2 spaces
      if (e.key === 'Tab') {
        e.preventDefault()
        const ta = textarea.value
        const start = ta.selectionStart
        const end = ta.selectionEnd
        const val = ta.value
        const newVal = val.substring(0, start) + '  ' + val.substring(end)
        emitUpdate({ rules: newVal })
        // Restore cursor position after Vue updates the value
        requestAnimationFrame(() => {
          ta.selectionStart = ta.selectionEnd = start + 2
        })
      }
      // Ctrl+Enter → evaluate
      if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) {
        e.preventDefault()
        emit('evaluate')
      }
    }

    function onDrop(e) {
      e.preventDefault()
      const text = e.dataTransfer.getData('text/plain')
      if (text) {
        // Convert backtick-wrapped column names to $col$ syntax
        let insertText = text
        if (insertText.startsWith('`') && insertText.endsWith('`')) {
          insertText = '$' + insertText.slice(1, -1) + '$'
        }
        const ta = textarea.value
        const start = ta.selectionStart
        const val = ta.value
        emitUpdate({ rules: val.substring(0, start) + insertText + val.substring(start) })
        requestAnimationFrame(() => {
          ta.selectionStart = ta.selectionEnd = start + insertText.length
          ta.focus()
        })
      }
    }

    function insertText(text) {
      const ta = textarea.value
      if (!ta) return
      ta.focus()
      const start = ta.selectionStart
      const end = ta.selectionEnd
      const val = ta.value
      const newVal = val.substring(0, start) + text + val.substring(end)
      emitUpdate({ rules: newVal })
      requestAnimationFrame(() => {
        // Place cursor after '(' — works for both "UPPER()" and "RPAD(, 10, ' ')"
        const parenIdx = text.indexOf('(')
        const newPos = parenIdx !== -1 ? start + parenIdx + 1 : start + text.length
        ta.selectionStart = ta.selectionEnd = newPos
      })
    }

    return {
      textarea, onRulesInput, onDefaultIsMissingChange, onDefaultValueInput,
      onModeChange, onOutputColumnNameInput, onReplaceColumnChange,
      onKeyDown, onDrop, insertText
    }
  }
}
</script>

<style scoped>
.rule-editor {
  display: flex;
  flex-direction: column;
  height: 100%;
  overflow: hidden;
  border: 1px solid #ddd;
  border-radius: 3px;
  background: #fff;
}
.rule-editor.compact {
  margin-bottom: 8px;
}

/* Header */
.editor-header {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 6px 10px;
  background: #fafafa;
  border-bottom: 1px solid #e0e0e0;
  flex-shrink: 0;
}
.editor-title {
  font-weight: 600;
  font-size: 12px;
  color: #555;
  text-transform: uppercase;
  letter-spacing: 0.5px;
}
.editor-hint {
  font-size: 11px;
  color: #999;
}

/* Rule textarea */
.editor-body {
  flex: 1;
  min-height: 0;
  display: flex;
  overflow: hidden;
}
.rule-textarea {
  width: 100%;
  height: 100%;
  padding: 8px 10px;
  font-family: "Consolas", "Monaco", "Courier New", monospace;
  font-size: 13px;
  line-height: 1.5;
  color: #333;
  border: none;
  outline: none;
  resize: none;
  background: #fff;
  tab-size: 2;
}
.rule-textarea::placeholder {
  color: #bbb;
}

/* Output configuration */
.output-config {
  border-top: 1px solid #e0e0e0;
  background: #f8f8f8;
  padding: 6px 10px;
  flex-shrink: 0;
}
.config-row {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 4px;
}
.config-row:last-child {
  margin-bottom: 0;
}
.config-label {
  display: flex;
  align-items: center;
  gap: 5px;
  font-size: 12px;
  color: #555;
  cursor: pointer;
  user-select: none;
}
.config-label input[type="checkbox"] {
  cursor: pointer;
}
.config-label-text {
  font-size: 12px;
  color: #555;
  white-space: nowrap;
  min-width: 90px;
}
.config-input {
  flex: 1;
  padding: 3px 8px;
  border: 1px solid #ddd;
  border-radius: 3px;
  font-size: 12px;
  outline: none;
  max-width: 300px;
}
.config-input:focus {
  border-color: #f8c900;
}
.config-select {
  flex: 1;
  padding: 3px 8px;
  border: 1px solid #ddd;
  border-radius: 3px;
  font-size: 12px;
  outline: none;
  max-width: 300px;
  background: #fff;
}
.config-select:focus {
  border-color: #f8c900;
}

/* Mode switch */
.mode-switch {
  display: flex;
  border: 1px solid #ddd;
  border-radius: 3px;
  overflow: hidden;
}
.mode-btn {
  padding: 3px 12px;
  font-size: 11px;
  font-weight: 600;
  border: none;
  background: #fff;
  color: #666;
  cursor: pointer;
  transition: all 0.15s;
}
.mode-btn:first-child {
  border-right: 1px solid #ddd;
}
.mode-btn.active {
  background: #f8c900;
  color: #333;
}
.mode-btn:hover:not(.active) {
  background: #f0f0f0;
}
</style>
