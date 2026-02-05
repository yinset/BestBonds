import numpy as np
import matplotlib.pyplot as plt

def foo(r, N):
    w = N / (1 + r) ** (N - 1) 
    w += np.sum([(i * r) / ((1 + r) ** i) for i in range(1, N)])
    return w

if __name__ == "__main__":
    r_range = np.linspace(0, 0.1, 1000)
    N = 30
    w = []
    for r in r_range:
        w.append([foo(r, n) for n in range(1, N+1)])
    h_list = []
    t_list = []
    q_list = []
    quantiality = 0.005
    for n in range(1, N+1):
        h = 0
        t = 0
        q = 0
        for r in r_range:
            t_w = foo(r, n)
            if abs(n / t_w - 2) < quantiality and h == 0:
                h = r
                h_list.append(r)
            elif abs(n / t_w - 3) < quantiality and t == 0:
                t = r
                t_list.append(r)
            elif abs(n / t_w - 4) < quantiality and q == 0:
                q = r
                q_list.append(r)
    hn = len(h_list)
    tn = len(t_list)
    qn = len(q_list)
    h_list = np.array(h_list)
    t_list = np.array(t_list)
    q_list = np.array(q_list)
    w = np.array(w)
    plt.contourf(np.linspace(1, N, N), r_range, w, levels=np.linspace(1, N, 2 * N))
    plt.plot(np.linspace(N - hn, N, hn), h_list, 'r')
    plt.plot(np.linspace(N - tn, N, tn), t_list, 'g')
    plt.plot(np.linspace(N - qn, N, qn), q_list, 'b')
    plt.colorbar()
    plt.show()