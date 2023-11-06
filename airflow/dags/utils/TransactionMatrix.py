import csv
import numpy as np
from scipy import sparse
from scipy.sparse import csr_array
from scipy.sparse import lil_matrix

class TransactionMatrix:
    
    def __init__(self, path, order=250000, is_h_matrix=False):
            
            num_filas = order
            num_columnas = order
            sparse_matrix_lil = lil_matrix((num_filas, num_columnas), dtype=float)
            sparse_matrix_lil_number_of_addresses = lil_matrix((num_filas, 1), dtype=float)
            value = 0

            with open(path, 'r') as csv_file:
                csv_reader = csv.reader(csv_file)
                next(csv_reader, None)
                for row in csv_reader:
                    if not is_h_matrix:
                        fila, columna = map(float, row)
                    else:
                        fila, columna, value = map(float, row)
                    # Actualiza la matriz LIL con el valor en la fila y columna adecuadas
                    sparse_matrix_lil[int(fila), int(columna)] = 1
                    # Tanto las filas como columnas son la propia traduccion del set {a1, a2} = posicion 23 ^ |{a1, a2}|=2 => (23, 23) = 2 
                    sparse_matrix_lil_number_of_addresses[int(fila), 0] = value

            sparse_matrix_csr = sparse_matrix_lil.tocsr()
            sparse_matrix_csr_number_of_addresses = sparse_matrix_lil_number_of_addresses.tocsr()

            self.matrix = sparse_matrix_csr
            self.matrix_number_of_addresses = sparse_matrix_csr_number_of_addresses

            # rows=[]
            # columns=[]
            # data=[]
            # number_addresses = dict()

            # csvreader = csv.reader(open(path))
            # next(csvreader, None)
            # for line in csvreader:
            #     if is_h_matrix:
            #         row, column, n_set_addresses = map(int, line)
            #         number_addresses[row] = n_set_addresses
            #     else:
            #         row, column = map(int, line)
            #     rows.append(row)
            #     columns.append(column)
            #     data.append(1)

            # self.matrix = sparse.csr_matrix((data, (rows, columns)), shape=(order, order))
            # self.number_addresses = number_addresses

    def get_matrix(self):
        return self.matrix
    
    def get_number_addresses(self):
        return self.matrix_number_of_addresses

    def get_shape(self):
        return self.matrix.shape

    def get_dense_matrix(self):
        return self.matrix.toarray()
    
    def matrix_product(self, matrix):
        return self.matrix.dot(matrix)