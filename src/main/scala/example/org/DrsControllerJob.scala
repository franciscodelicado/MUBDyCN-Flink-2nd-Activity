/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package example.org

object DrsControllerJob {
  def main(args: Array[String]) {

    // 1.- set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    /* TODO: Añadir un Source de tipo OrionSource sobre el puerto 9001    
    *     Nota: La el tiempo deberá ser extraido de los datos enviados por Orion
    */

    // TODO: Obtener las telemetrías del coche


    // TODO: Calcular la aceleración del coche a partir de las telemetrías obtenidas

    // TODO: Guardar en Cassandra tanto las telemetrías obtenidas como la aceleración calculada

    /* TODO: Implementar el controlador de DRS:
    *     - Si velocidad > 200 km/h y el freno no está presionado => activar DRS
    *     - Si freno presionado o velocidad < 200 km/h => desactivar DRS
    *     - En caso de que el DRS ya esté activado, no enviar comandos redundantes a Orion
    *   Nota: Para enviar comandos a Orion, se puede usar un Sink de tipo OrionSink. 
    *         El formato de los comandos dependerá de la definición de las entidades en Orion.
    */

    
    env.execute("DRS Controller Job")

  }

  