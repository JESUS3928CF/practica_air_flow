import random
import tkinter as tk
from tkinter import ttk

def generar_numero_aleatorio(longitud):
    numero_aleatorio = random.randint(0, 10 ** longitud - 1)
    numero_aleatorio_str = f"{numero_aleatorio:0{longitud}}"
    return numero_aleatorio_str

def generar_archivo():
    nit_entidad = entry_nit.get().ljust(10)[:10]
    fecha_recaudo = entry_fecha.get()[:8]
    codigo_banco = entry_banco.get().zfill(3)[:3]
    fecha_hora_creacion_modificador = entry_fecha_hora.get().ljust(15)[:15]
    codigos_referencia_factura = entry_codigos.get().split(',')
    valores_recaudados = entry_valores.get().split(',')

    if len(codigos_referencia_factura) != len(valores_recaudados):
        result_label.config(text="Las facturas y valores no coinciden")
        return

    num_operacion_autorizacion = generar_numero_aleatorio(12)
    cabecera_lote5 = generar_numero_aleatorio(17)
    control_lote8_y_9 = generar_numero_aleatorio(4)
    numero_aleatorio = generar_numero_aleatorio(1)

    lineas = []
    lineas.append(f"01{nit_entidad.strip()}{fecha_recaudo.strip()}{codigo_banco.strip()}00000379400000574{fecha_hora_creacion_modificador.strip()}{' ' * 107}"[:163])
    lineas.append(f"05{cabecera_lote5.strip()}{' ' * 143}"[:163])

    for codigo, valor in zip(codigos_referencia_factura, valores_recaudados):
        codigo = codigo.ljust(11)[:11]
        valor = valor.zfill(12)[:12]
        linea_06 = f"06{'0' * 44}{codigo.strip()}{valor.strip()}000101{num_operacion_autorizacion.strip()}05133320000002{' ' * 68}"[:163]
        lineas.append(linea_06)

    lineas.append(f"08{'0' * 8}{numero_aleatorio}{'0' * 10}{control_lote8_y_9}{'0' * 7}1{' ' * 129}"[:163])
    lineas.append(f"09{'0' * 8}{numero_aleatorio}{'0' * 10}{control_lote8_y_9}{'0' * 4}{' ' * 133}"[:163])

    with open("Pago de la Asociación Bancaria.txt", "w") as archivo:
        for linea in lineas:
            archivo.write(linea + "\n")

    result_label.config(text="El pago asobancario se ha creado con éxito.")

root = tk.Tk()
root.title("Generador de pagos")
root.geometry("900x520")

# Estilo para una apariencia más relajante
style = ttk.Style()
style.theme_use("clam")

style.configure("TFrame", background="#f0f0f0", borderwidth=2, relief="solid")
style.configure("TLabel", font=("Helvetica", 12), background="#f0f0f0", foreground="#333333")
style.configure("TEntry", font=("Helvetica", 12), padding=5, foreground="#333333", fieldbackground="#ffffff", borderwidth=1, relief="solid")
style.configure("TButton", font=("Helvetica", 12, "bold"), padding=10, foreground="#ffffff", background="#007AFF", borderwidth=0)
style.map("TButton", background=[("active", "#005BB5")])

frame = ttk.Frame(root, padding="20 20 20 20", style="TFrame")
frame.pack(pady=20)

# Agregar un label con el nombre del autor en la esquina derecha inferior
label_author = ttk.Label(root, text="Juan Cochero", font=("Helvetica", 4), background=root.cget('bg'), foreground="#AAAAAA")
label_author.place(relx=1.0, rely=1.0, anchor='se', x=-10, y=-10)

def create_labeled_entry(parent, label_text, row, column, width=30):
    label = ttk.Label(parent, text=label_text, style="TLabel")
    label.grid(row=row, column=column, padx=10, pady=10, sticky="w")
    entry = ttk.Entry(parent, width=width, style="TEntry")
    entry.grid(row=row, column=column+1, padx=10, pady=10)
    return entry

# Crear entradas con etiquetas
entry_nit = create_labeled_entry(frame, "NIT de la entidad (10 caracteres):", 0, 0)
entry_fecha = create_labeled_entry(frame, "Fecha del recaudo (AAAAMMDD):", 1, 0)
entry_banco = create_labeled_entry(frame, "Código del banco (3 caracteres):", 2, 0)
entry_fecha_hora = create_labeled_entry(frame, "Fecha, hora de creación más modificador (AAAAMMDDHHMMZNN):", 3, 0)
entry_codigos = create_labeled_entry(frame, "Códigos de referencia de factura (separados por comas):", 4, 0)
entry_valores = create_labeled_entry(frame, "Total del valor recaudado correspondiente (separados por comas):", 5, 0)

# Botón para generar el archivo
btn_generar = ttk.Button(frame, text="Generar Pago", style="TButton", command=generar_archivo)
btn_generar.grid(row=6, columnspan=2, pady=20)

# Etiqueta para mostrar resultados
result_label = ttk.Label(frame, text="", style="TLabel")
result_label.grid(row=7, columnspan=2, pady=10)

root.mainloop()