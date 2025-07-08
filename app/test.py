import pyqtgraph as pg
import pyqtgraph.opengl as gl

# Create a QApplication first
pg.mkQApp()

# Create GL view
w = gl.GLViewWidget()
w.show()

# Add a grid so you see something
g = gl.GLGridItem()
w.addItem(g)

print("If this window appeared without error, OpenGL is working.")

pg.exec()