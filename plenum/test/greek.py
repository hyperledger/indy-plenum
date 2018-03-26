from collections import namedtuple

Greek = namedtuple('Greek', ['name', 'upper', 'lower'])

Greeks = [
    Greek("Alpha", "A", "α"),
    Greek("Beta", "B", "β"),
    Greek("Gamma", "Γ", "γ"),
    Greek("Delta", "∆", "δ"),
    Greek("Epsilon", "E", "ε"),
    Greek("Zeta", "Z", "ζ"),
    Greek("Eta", "H", "η"),
    Greek("Theta", "Θ", "θ"),
    Greek("Iota", "I", "ι"),
    Greek("Kappa", "K", "κ"),
    Greek("Lambda", "Λ", "λ"),
    Greek("Mu", "M", "µ"),
    Greek("Nu", "N", "ν"),
    Greek("Xi", "Ξ", "ξ"),
    Greek("Omicron", "O", "o"),
    Greek("Pi", "Π", "π"),
    Greek("Rho", "P", "ρ"),
    Greek("Sigma", "Σ", "σ"),
    Greek("Tau", "T", "τ"),
    Greek("Upsilon", "Υ", "υ"),
    Greek("Phi", "Φ", "φ"),
    Greek("Chi", "X", "χ"),
    Greek("Psi", "Ψ", "ψ"),
    Greek("Omega", "Ω", "ω")
]


def genNodeNames(nodeCount):
    return [g.name for g in Greeks[:nodeCount]]
