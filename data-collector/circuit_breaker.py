import time
import threading

class CircuitBreaker:
    def __init__(self, failure_threshold=5, recovery_timeout=30, expected_exception=Exception):
        self.failure_threshold = failure_threshold          # Soglia superata la quale si apre il circuito
        self.recovery_timeout = recovery_timeout            # Timeout prima di provare a resettare il circuito
        self.expected_exception = expected_exception        # Tipo di eccezione da monitorare
        self.failure_count = 0
        self.last_failure_time = None                       # Timestamp per ultimo fallimento
        self.state = 'CLOSED'                               # Stato iniziale del sistema
        self.lock = threading.Lock()                        # Operazioni thread-safe

    def call(self, func, *args, **kwargs):
        with self.lock:
            if self.state == 'OPEN':
                time_since_failure = time.time() - self.last_failure_time # Tempo passato dall'ultimo fallimento

                if time_since_failure > self.recovery_timeout:
                    self.state = 'HALF_OPEN' # Passiamo allo stato HALF_OPEN dopo il recovery timeout
                else:
                    raise CircuitBreakerOpenException("Il circuito è aperto. Chiamata rifiutata")

            try:
                # Proviamo ad eseguire la funzione
                result = func(*args, **kwargs)
            except self.expected_exception as e:
                self.failure_count += 1 # Incrementiamo il contatore dei fallimenti

                self.last_failure_time = time.time()  # Aggiorniamo il timestamp per l'ultimo fallimento

                # Apro il circuito se la soglia dei fallimenti è stata raggiunta
                if self.failure_count >= self.failure_threshold:
                    self.state = 'OPEN'
                raise e  # Generiamo di nuovo l'eccezione per il chiamante
            else:
                # Funzione eseguita con successo
                if self.state == 'HALF_OPEN':
                    self.state = 'CLOSED' # Successo in HALF_OPEN --> CLOSED
                    self.failure_count = 0
                return result

# Eccezione generata quando il circuito è OPEN
class CircuitBreakerOpenException(Exception):
    pass

