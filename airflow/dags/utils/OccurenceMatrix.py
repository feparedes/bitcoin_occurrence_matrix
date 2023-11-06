import numpy as np

class OccurenceMatrix:

    def __init__(self, m_matrix, number_addresses):
        
        self.m_matrix = m_matrix

        occurence_matrix = np.zeros((20,20))

        for row in range(1, 21):
            for column in range(1,21):
                # Numero de conjuntos con el mismo cardinal en cuanto a numero de direcciones (addresses)
                # Candidatos, luego hay que ver cuales de estas direcciones tienen valor == column
                indices_cardinal = np.where(number_addresses.data==row)
                                
                if len(indices_cardinal[0]) == 0:
                    final_value = 0
                else:
                    # Suma de la matriz M por columnas del conjunto de direcciones
                    columns_sum_m_matrix = m_matrix.sum(axis=1)
                    sum_values_set_of_addresses = np.take(columns_sum_m_matrix, indices_cardinal)

                    # Return a 2D matrix where the important values are in the columns
                    final_value = len(np.where(sum_values_set_of_addresses==column)[1])
                
                occurence_matrix[row-1,column-1]=final_value

        self.occurence_matrix = occurence_matrix


    def get_occurrence_matrix(self):
        return self.occurence_matrix
    
    def print_occurrence_matrix(self):
        print(f"Occurence matrix \n{self.occurence_matrix.astype(int)}")

    def save_occurrence_matrix(self, path):
        # np.savetxt('out/occurrence_matrix.out', occurence_matrix, fmt = '%2.0f')
        np.savetxt(path, self.occurence_matrix, fmt = '%2.0f')
