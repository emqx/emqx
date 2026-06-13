import { setSchemaEditorValue } from './schema_editor.js';
import { loadConnections, saveConnection, editConnection, deleteConnection, startConnection, stopConnection, resetConnectionEditor } from './connections.js';
import { loadTools, saveTool, editTool, deleteTool, updateToolForm, resetToolEditor, defaultPublishInputSchema } from './tools.js';
import { loadProfiles } from './providers.js';
import { loadPipelines, savePipeline, editPipeline, deletePipeline, togglePipelineActive, resetPipelineEditor } from './pipelines.js';
import { addStep, removeStep, moveStep, stepTypeChanged, addKV } from './pipeline_steps.js';
import { setStatus, showTab, toast } from './ui_helpers.js';

window.addEventListener('DOMContentLoaded', async () => {
  document.getElementById('endpoint-label').textContent = window.location.host;
  setSchemaEditorValue('se-tool-publish-input', defaultPublishInputSchema());
  setSchemaEditorValue('se-tool-request-payload-schema', null);
  setSchemaEditorValue('se-tool-input-schema', null);
  await refresh();
});

async function refresh() {
  try {
    await Promise.all([loadConnections(), loadTools(), loadProfiles(), loadPipelines()]);
    setStatus('ok');
  } catch(e) {
    setStatus('err');
    toast('Load failed: ' + e.message, 'err');
  }
}

// Expose handlers for inline onclick attributes
window.showTab = showTab;
window.saveTool = saveTool;
window.editTool = editTool;
window.deleteTool = deleteTool;
window.updateToolForm = updateToolForm;
window.resetToolEditor = resetToolEditor;
window.saveConnection = saveConnection;
window.editConnection = editConnection;
window.deleteConnection = deleteConnection;
window.startConnection = startConnection;
window.stopConnection = stopConnection;
window.resetConnectionEditor = resetConnectionEditor;
window.savePipeline = savePipeline;
window.editPipeline = editPipeline;
window.deletePipeline = deletePipeline;
window.togglePipelineActive = togglePipelineActive;
window.resetPipelineEditor = resetPipelineEditor;
window.addStep = addStep;
window.removeStep = removeStep;
window.moveStep = moveStep;
window.stepTypeChanged = stepTypeChanged;
window.addKV = addKV;
