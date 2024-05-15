library(taskscheduleR)

# Definir un horario para cada hora del día, en el minuto 01
horarios <- sprintf("%02d:01", 0:23)

for (hora in horarios) {
  taskscheduler_create(
    taskname = paste("Tarea_prueba", gsub(":", "", hora), sep = "_"),
    rscript = "C:/Users/abrios/Desktop/alerta03/Alertas_ECA/alertas7.R",
    schedule = "DAILY",
    starttime = hora
  )
}

# Listar las tareas para verificar
taskscheduler_ls()

# Bloque para eliminar las tareas, mantén comentado hasta que necesites usarlo
for (hora in horarios) {
  nombre_tarea <- paste("Tarea_prueba", gsub(":", "", hora), sep = "_")
  taskscheduler_delete(taskname = nombre_tarea)
}
