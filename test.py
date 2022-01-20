import types
import enet

print([getattr(enet, a) for a in dir(enet)
  if isinstance(getattr(enet, a), types.FunctionType)])