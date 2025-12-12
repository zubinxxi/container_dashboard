from database import run_query
from datetime import date

class MovimientoContenedoresModel:
    def __init__(self, id = "", cliente ="", tel = "", proyecto = "", fecha = date.today()):
        self.id = id
        self.cliente = cliente
        self.tel = tel
        self.proyecto = proyecto
        self.fecha = fecha

    @staticmethod
    def get_all_dataframe():
        return [dict(row) for row in  run_query("SELECT * FROM clientes ") ]
