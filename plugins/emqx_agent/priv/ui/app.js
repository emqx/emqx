import { setSchemaEditorValue } from './schema_editor.js';
import { loadConnections, saveConnection, editConnection, deleteConnection, startConnection, stopConnection, resetConnectionEditor } from './connections.js';
import { loadSkills, saveSkill, editSkill, deleteSkill, updateSkillForm, resetSkillEditor, defaultPublishInputSchema } from './skills.js';
import { loadProfiles } from './providers.js';
import { loadPipelines, savePipeline, editPipeline, deletePipeline, togglePipelineActive, resetPipelineEditor } from './pipelines.js';
import { addStep, removeStep, moveStep, stepTypeChanged, addKV } from './pipeline_steps.js';
import { setStatus, showTab, toast } from './ui_helpers.js';

window.addEventListener('DOMContentLoaded', async () => {
  document.getElementById('endpoint-label').textContent = window.location.host;
  setSchemaEditorValue('se-skill-publish-input', defaultPublishInputSchema());
  setSchemaEditorValue('se-skill-request-payload-schema', null);
  setSchemaEditorValue('se-skill-input-schema', null);
  await refresh();
});

async function refresh() {
  try {
    await Promise.all([loadConnections(), loadSkills(), loadProfiles(), loadPipelines()]);
    setStatus('ok');
  } catch(e) {
    setStatus('err');
    toast('Load failed: ' + e.message, 'err');
  }
}

// Expose handlers for inline onclick attributes
window.showTab = showTab;
window.saveSkill = saveSkill;
window.editSkill = editSkill;
window.deleteSkill = deleteSkill;
window.updateSkillForm = updateSkillForm;
window.resetSkillEditor = resetSkillEditor;
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
