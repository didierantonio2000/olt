
// Limpiar flag de operación
async function cleanUopFlag() {
    if (!currentOntForModal || !currentOltId) return;
    const { pon, ont } = currentOntForModal;
    if (!confirm(`¿Limpiar flag de operación en ONT ${pon}/${ont}?`)) return;
    try {
        const r = await fetch(`/api/cleanuopflag/${currentOltId}/${pon}/${ont}`, { method: 'POST' });
        const d = await r.json();
        showNotification(d.success ? 'Flag limpiado' : d.message, d.success ? 'success' : 'error');
    } catch { showNotification('Error al limpiar flag', 'error'); }
}

// Guardar nombre de ONT
async function saveOntName() {
    if (!currentOntForModal || !currentOltId) return;
    const { pon, ont } = currentOntForModal;
    const name = document.getElementById('edit-ont-name').value.trim();
    if (!name) { showNotification('Ingrese un nombre', 'warning'); return; }
    try {
        const r = await fetch('/api/ont/name', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ oltId: currentOltId, pon, ont, name })
        });
        const d = await r.json();
        showNotification(d.success ? 'Nombre guardado' : d.message, d.success ? 'success' : 'error');
        if (d.success) {
            currentOntForModal.name = name;
            renderTable();
        }
    } catch { showNotification('Error al guardar nombre', 'error'); }
}

// Iniciar
init();
</script>
</body>
</html>
