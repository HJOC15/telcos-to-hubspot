# Usa una imagen base de Node.js
FROM node:18

# Establece el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copia los archivos de dependencias del proyecto
COPY package*.json /app/

# Instala las dependencias
RUN npm install


# Copia el código fuente de la aplicación
COPY . /app/

# COPY ./.env.prod /app/.env
#COPY ./.env.production /app/.env

# Expone el puerto que utiliza tu aplicación
EXPOSE 3000

CMD ["npm", "start"]

#ENTRYPOINT [ "./startup.sh" ] 